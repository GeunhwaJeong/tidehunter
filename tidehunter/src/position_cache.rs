//! Position-keyed cache of immutable WAL frame content.
//!
//! Two caches in `Db` are instances of [`PositionCache`]:
//!
//! - the decompressed-batch cache (`Config::compressed_batch_cache_bytes`).
//!   Every key written in a compressed batch shares the `WalPosition` of the
//!   batch frame, so point reads, iterators and relocation repeatedly land on
//!   the same frame and each pay a full frame read plus a full-body
//!   decompression to extract one record. The cache keys decompressed bodies
//!   by the frame's WAL offset.
//! - the index blob cache (`Config::index_blob_cache_bytes`, see
//!   `index_blob_cache`). Whole index blobs that have aged out of the index
//!   WAL's mmap window are kept by their WAL offset, so lookups into an
//!   oversized micro-cell section stop copying the section out of the file
//!   on every miss.
//!
//! Both rest on the same property: within a single open `Db` instance a WAL
//! offset identifies immutable frame content — the cache lives only in
//! memory and starts empty at open, and the crash-replay rewind (which can
//! rewrite tail offsets) happens before the `Db` is constructed. The cache
//! must never be persisted or shared across `Db` instances; offsets are
//! *not* unique across restarts.
//!
//! Reachability: a WAL read of a garbage-collected position returns
//! `Ok(None)`, and readers rely on that as deletion semantics for stale
//! index entries (which survive until a promote rewrites the index). A
//! cached body must not override it, so the cache owns the check: every
//! body served by [`PositionCache::get`] or [`PositionCache::peek`] is
//! re-validated against the `reachable` predicate supplied at construction
//! (`Wal::is_reachable`), and bodies whose WAL file was reclaimed are
//! dropped instead of served. Callers cannot bypass this. One caveat is
//! inherent to caching: a warm hit skips the CRC-validating frame read, so
//! on-disk corruption that appears *after* a body was cached is masked
//! (reads keep succeeding with the correct data) until the entry is evicted
//! or the process restarts — the uncached engine would surface a CRC error
//! instead.
//!
//! Read paths consult the cache *before* reading the WAL frame
//! ([`PositionCache::get`], or [`PositionCache::peek`] where blocking is not
//! allowed), so a hit skips the frame I/O, CRC pass and copy as well as any
//! decompression. On a miss the caller enters
//! [`PositionCache::get_or_fill`] with a closure that produces the body
//! (decompressing a frame it already read, or reading the blob from the
//! file), which single-flights the fill: one leader runs the closure while
//! concurrent readers of the same position block on its result
//! (`wait_mcs` counts that wait). A reader that arrives while a fill is in
//! flight waits in `get` too — a `Pending` slot proves the body is already
//! coming, so its own frame read would be wasted. Waiters release their own
//! fill closure (and whatever frame copy it captured) before blocking; if
//! the leader fails (panics on a corrupt frame or a read error), they
//! report [`CacheOutcome::RetryRead`] and the caller re-reads the frame.
//! The trade-off is deliberate: waiters bound their latency to the leader's
//! wall-clock fill (no timeout), which eliminates N-1 duplicate frame reads
//! and fills but lets a descheduled leader correlate waiter tail latency.
//!
//! Eviction is second-chance (CLOCK) rather than strict LRU: a hit only
//! sets an atomic `referenced` bit under a shard *read* lock, so concurrent
//! hits on one hot position do not serialize, and one-touch bodies streamed
//! in by scans (relocation, iterators) are evicted before point-read entries
//! that keep re-setting their bit. Single-flight waiters count as hits for
//! this purpose — after a fill they set the bit too, so a burst-read
//! position ranks as hot. This is also why `primitives::ShardedMutex` is not
//! reused here — the hit path needs a shared (read) lock, not a mutex; the
//! shard locks mirror its acquisition protocol instead (try-lock fast path,
//! then `runtime::block_in_place` with the `contention` histogram timing
//! the blocking acquisition).
//!
//! The byte budget is accounted globally across shards and bounded by total
//! body bytes rather than entry count, because bodies span several orders of
//! magnitude in size. A fill inserts first and then sweeps shards
//! round-robin starting after its own, so eviction pressure drains
//! one-touch garbage anywhere in the cache rather than thrashing the
//! inserting shard; retained bytes can transiently exceed the budget by the
//! bodies of in-flight fills until their sweeps settle. A body larger than
//! the whole budget is returned to the caller but never cached (`rejected`
//! counts these). Owners can also drop a body explicitly with
//! [`PositionCache::remove`] once nothing references its position any more
//! (the index blob cache does this when a flush replaces a cell's levels);
//! a fill in flight for such a position still serves its waiters but is not
//! retained.
use crate::metrics::{MetricHistogram, MetricIntCounter, MetricIntGauge, TimerExt};
use crate::runtime;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use parking_lot::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Answers whether the frame at a position is still readable from the WAL.
/// Supplied by `Db::open` as `Wal::is_reachable`; tests substitute stubs.
pub(crate) type ReachabilityCheck = Box<dyn Fn(WalPosition) -> bool + Send + Sync>;

/// Metric handles for one [`PositionCache`] instance. Each cache registers
/// its own set under its own metric names (see
/// `Metrics::decompress_cache_metrics` and `Metrics::index_blob_cache_metrics`).
pub(crate) struct PositionCacheMetrics {
    /// Total body bytes currently retained, across all shards.
    pub bytes: MetricIntGauge,
    /// Bodies returned to a caller but not cached because they exceed the
    /// whole budget.
    pub rejected: MetricIntCounter,
    /// Cumulative microseconds callers spent blocked on another thread's
    /// in-flight fill of the same position.
    pub wait_mcs: MetricIntCounter,
    /// Time to acquire a contended shard lock; uncontended acquisitions are
    /// not recorded.
    pub contention: MetricHistogram,
    /// Bodies evicted by the CLOCK sweep to stay under the budget.
    pub evictions: MetricIntCounter,
}

pub(crate) struct PositionCache {
    shards: Box<[RwLock<Shard>]>,
    max_bytes: usize,
    /// Total body bytes retained across all shards; mirrored into the
    /// `metrics.bytes` gauge.
    bytes: AtomicUsize,
    /// Total slots across all shards, `Pending` fills included — the
    /// `is_empty` gate, so pre-I/O probes resume the moment a first fill is
    /// in flight (its body has no bytes yet) and waiters can join it.
    occupied: AtomicUsize,
    reachable: ReachabilityCheck,
    metrics: PositionCacheMetrics,
}

#[derive(Default)]
struct Shard {
    slots: HashMap<u64, Slot>,
    /// Second-chance eviction ring; holds exactly the `Ready` offsets of
    /// this shard, once each. `Pending` slots are never evictable.
    clock: VecDeque<u64>,
}

enum Slot {
    Ready(CachedBody),
    /// A leader thread is filling this position; readers block on the
    /// cell until the leader publishes the body (or fails).
    Pending(Arc<FillCell>),
}

struct CachedBody {
    body: Bytes,
    referenced: AtomicBool,
}

#[derive(Default)]
struct FillCell {
    state: Mutex<FillState>,
    cv: Condvar,
    /// Set by [`PositionCache::remove`] while the fill is in flight: the
    /// owner has stopped referencing the position, so the leader hands the
    /// body to its waiters but does not retain it.
    retired: AtomicBool,
}

#[derive(Default)]
enum FillState {
    #[default]
    Filling,
    Done(Bytes),
    /// The leader panicked (corrupt frame, read error); waiters re-read
    /// and retry.
    Failed,
}

impl FillCell {
    fn publish(&self, state: FillState) {
        *self.state.lock() = state;
        self.cv.notify_all();
    }
}

/// Result of [`PositionCache::get_or_fill`]. `Hit` means this thread did
/// not run the fill (cached body, or waited on another thread's in-flight
/// fill); `Filled` means it did. `RetryRead` means the leader this thread
/// waited on failed after the thread had already released its fill closure
/// — the caller must rebuild the closure (re-read the frame) and call
/// again.
pub(crate) enum CacheOutcome {
    Hit(Bytes),
    Filled(Bytes),
    RetryRead,
}

impl CacheOutcome {
    #[cfg(test)]
    pub fn into_body(self) -> Bytes {
        match self {
            CacheOutcome::Hit(body) | CacheOutcome::Filled(body) => body,
            CacheOutcome::RetryRead => panic!("RetryRead carries no body"),
        }
    }
}

/// Removes the leader's `Pending` slot and publishes `Failed` if the fill
/// closure unwinds, so waiters do not hang on a dead leader.
struct FillGuard<'a> {
    cache: &'a PositionCache,
    position: WalPosition,
    cell: &'a Arc<FillCell>,
    completed: bool,
}

impl Drop for FillGuard<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut shard = self.cache.write_shard(self.cache.shard(self.position));
        if let Some(Slot::Pending(pending)) = shard.slots.get(&self.position.offset())
            && Arc::ptr_eq(pending, self.cell)
        {
            shard.slots.remove(&self.position.offset());
            let _: usize = self.cache.occupied.fetch_sub(1, Ordering::Relaxed);
        }
        drop(shard);
        self.cell.publish(FillState::Failed);
    }
}

/// Shard count scaled to the host: every WAL-resolving read probes a shard
/// lock once the cache is non-empty, so on many-core readers 8 lock words
/// would become a coherence hot spot. Power of two, clamped to [8, 64].
fn shard_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
        .next_power_of_two()
        .clamp(8, 64)
}

impl PositionCache {
    pub fn new(
        max_bytes: usize,
        reachable: ReachabilityCheck,
        metrics: PositionCacheMetrics,
    ) -> Self {
        assert!(max_bytes > 0, "position cache budget must be non-zero");
        let shards = (0..shard_count()).map(|_| RwLock::default()).collect();
        Self {
            shards,
            max_bytes,
            bytes: AtomicUsize::new(0),
            occupied: AtomicUsize::new(0),
            reachable,
            metrics,
        }
    }

    fn shard_index(&self, position: WalPosition) -> usize {
        // Fibonacci hashing: WAL offsets are aligned, so low bits are biased.
        let hash = position.offset().wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (hash >> 32) as usize % self.shards.len()
    }

    fn shard(&self, position: WalPosition) -> &RwLock<Shard> {
        &self.shards[self.shard_index(position)]
    }

    /// Acquires a shard read lock, mirroring `ShardedMutex`'s protocol: an
    /// uncontended try-lock fast path, then a blocking acquisition under
    /// `runtime::block_in_place` (so a tokio worker is not parked) timed by
    /// the contention histogram.
    fn read_shard<'a>(&self, lock: &'a RwLock<Shard>) -> RwLockReadGuard<'a, Shard> {
        if let Some(guard) = lock.try_read() {
            return guard;
        }
        let _timer = self.metrics.contention.clone().mcs_timer();
        runtime::block_in_place(|| lock.read())
    }

    /// Write-lock analog of [`Self::read_shard`].
    fn write_shard<'a>(&self, lock: &'a RwLock<Shard>) -> RwLockWriteGuard<'a, Shard> {
        if let Some(guard) = lock.try_write() {
            return guard;
        }
        let _timer = self.metrics.contention.clone().mcs_timer();
        runtime::block_in_place(|| lock.write())
    }

    /// Pre-I/O lookup: returns the body for `position` without touching
    /// the WAL, waiting on an in-flight fill if one is running. Bodies are
    /// served only while the frame is still reachable; a body whose WAL
    /// file was reclaimed is dropped and reported as a miss, preserving the
    /// read path's `None`-for-GC'd deletion semantics. `None` means the
    /// caller must produce the body itself, normally through
    /// [`Self::get_or_fill`].
    pub fn get(&self, position: WalPosition) -> Option<Bytes> {
        let cell = {
            let shard = self.read_shard(self.shard(position));
            match shard.slots.get(&position.offset()) {
                Some(Slot::Ready(cached)) => {
                    cached.referenced.store(true, Ordering::Relaxed);
                    let body = cached.body.clone();
                    drop(shard);
                    return self.check_reachable(position, body);
                }
                Some(Slot::Pending(cell)) => cell.clone(),
                None => return None,
            }
        };
        let body = self.wait_and_touch(position, &cell)?;
        self.check_reachable(position, body)
    }

    /// Non-blocking variant of [`Self::get`] for callers that hold a lock
    /// the fill must not stall (the row mutex in `Db::index_reader`): a
    /// resident body is served and touched exactly as by `get`, while an
    /// in-flight fill is reported as a miss instead of waited on. The
    /// caller then joins that fill through [`Self::get_or_fill`] once the
    /// lock is released.
    pub fn peek(&self, position: WalPosition) -> Option<Bytes> {
        let body = {
            let shard = self.read_shard(self.shard(position));
            match shard.slots.get(&position.offset()) {
                Some(Slot::Ready(cached)) => {
                    cached.referenced.store(true, Ordering::Relaxed);
                    cached.body.clone()
                }
                Some(Slot::Pending(_)) | None => return None,
            }
        };
        self.check_reachable(position, body)
    }

    /// Serves `body` only if the frame is still readable; otherwise drops
    /// the stale entry and reports a miss.
    fn check_reachable(&self, position: WalPosition, body: Bytes) -> Option<Bytes> {
        if (self.reachable)(position) {
            Some(body)
        } else {
            self.remove(position);
            None
        }
    }

    /// True when the cache holds nothing — no bodies and no in-flight
    /// fills. Lets read paths skip the shard-lock probe entirely on
    /// databases where the cache never fills (e.g. no compressed frames are
    /// being read, or no index blob ever qualifies for admission).
    pub fn is_empty(&self) -> bool {
        self.occupied.load(Ordering::Relaxed) == 0
    }

    /// The byte budget; a single body larger than this is never retained.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Drops the cached body for `position`, either because the frame's
    /// WAL file was reclaimed (the stale body must stop being served) or
    /// because the owner knows nothing references the position any more
    /// (an index blob superseded by a flush). Returns whether a resident
    /// body was dropped. An in-flight fill is marked retired instead: its
    /// leader started from a position that was referenced at the time, so
    /// the body is still correct for its waiters, but it is not retained
    /// afterwards — otherwise an unreferenced body would sit in the cache,
    /// touched by those waiters, until the CLOCK sweep got to it.
    pub fn remove(&self, position: WalPosition) -> bool {
        let mut shard = self.write_shard(self.shard(position));
        match shard.slots.get(&position.offset()) {
            Some(Slot::Ready(_)) => {}
            Some(Slot::Pending(cell)) => {
                cell.retired.store(true, Ordering::Relaxed);
                return false;
            }
            None => return false,
        }
        let Some(Slot::Ready(cached)) = shard.slots.remove(&position.offset()) else {
            unreachable!("checked above");
        };
        shard.clock.retain(|offset| *offset != position.offset());
        let _: usize = self.occupied.fetch_sub(1, Ordering::Relaxed);
        self.sub_bytes(cached.body.len());
        true
    }

    /// Miss-path entry point for a position the caller has already resolved
    /// in the WAL — a `CompressedBatch` frame it just read, or an index blob
    /// whose file reader it opened under the row lock — which is why the
    /// reachability predicate is not re-checked here. Returns the body from
    /// the cache, or runs `fill` as the single leader across all concurrent
    /// callers for this position while the rest wait on its result. Waiters
    /// release `fill` (and any frame copy it captures) before blocking, so a
    /// failed leader surfaces as [`CacheOutcome::RetryRead`].
    pub fn get_or_fill<F: FnOnce() -> Bytes>(
        &self,
        position: WalPosition,
        fill: F,
    ) -> CacheOutcome {
        let cell = {
            let mut shard = self.write_shard(self.shard(position));
            match shard.slots.get(&position.offset()) {
                Some(Slot::Ready(cached)) => {
                    cached.referenced.store(true, Ordering::Relaxed);
                    return CacheOutcome::Hit(cached.body.clone());
                }
                Some(Slot::Pending(cell)) => cell.clone(),
                None => {
                    let cell = Arc::new(FillCell::default());
                    shard
                        .slots
                        .insert(position.offset(), Slot::Pending(cell.clone()));
                    let _: usize = self.occupied.fetch_add(1, Ordering::Relaxed);
                    drop(shard);
                    return CacheOutcome::Filled(self.lead_fill(position, cell, fill));
                }
            }
        };
        // Release the caller's fill closure (and the frame copy it may hold)
        // before blocking, so N waiters do not pin N frame copies for the
        // leader's whole fill.
        drop(fill);
        match self.wait_and_touch(position, &cell) {
            Some(body) => CacheOutcome::Hit(body),
            None => CacheOutcome::RetryRead,
        }
    }

    /// Runs `fill` as the leader for `position`, then swaps the `Pending`
    /// slot for the cached body, wakes waiters, and sweeps shards back under
    /// the byte budget.
    fn lead_fill<F: FnOnce() -> Bytes>(
        &self,
        position: WalPosition,
        cell: Arc<FillCell>,
        fill: F,
    ) -> Bytes {
        let mut guard = FillGuard {
            cache: self,
            position,
            cell: &cell,
            completed: false,
        };
        let body = fill();
        {
            let mut shard = self.write_shard(self.shard(position));
            if cell.retired.load(Ordering::Relaxed) {
                // Nothing references the position any more: serve the
                // waiters, retain nothing.
                shard.slots.remove(&position.offset());
                let _: usize = self.occupied.fetch_sub(1, Ordering::Relaxed);
            } else if body.len() <= self.max_bytes {
                // Replaces our Pending slot: `occupied` is unchanged.
                shard.slots.insert(
                    position.offset(),
                    Slot::Ready(CachedBody {
                        body: body.clone(),
                        // Second chance starts un-referenced: a one-touch
                        // scan body is evicted before re-read entries.
                        // Waiters set the bit on their way out.
                        referenced: AtomicBool::new(false),
                    }),
                );
                shard.clock.push_back(position.offset());
                self.add_bytes(body.len());
            } else {
                shard.slots.remove(&position.offset());
                let _: usize = self.occupied.fetch_sub(1, Ordering::Relaxed);
                self.metrics.rejected.inc();
            }
        }
        guard.completed = true;
        cell.publish(FillState::Done(body.clone()));
        // Sweep from the next shard so the just-inserted body is the last
        // eviction candidate; the round-robin start also spreads eviction
        // pressure across shards instead of thrashing the hot one.
        self.trim((self.shard_index(position) + 1) % self.shards.len());
        body
    }

    /// Blocks until the in-flight fill for `position` resolves, then marks
    /// the cached body referenced — the waiter is a read, and without the
    /// touch a burst-read frame would rank below a once-probed cold entry.
    fn wait_and_touch(&self, position: WalPosition, cell: &FillCell) -> Option<Bytes> {
        let body = {
            let _timer = self.metrics.wait_mcs.clone().mcs_timer();
            runtime::block_in_place(|| {
                let mut state = cell.state.lock();
                loop {
                    match &*state {
                        FillState::Filling => cell.cv.wait(&mut state),
                        FillState::Done(body) => return Some(body.clone()),
                        FillState::Failed => return None,
                    }
                }
            })
        }?;
        let shard = self.read_shard(self.shard(position));
        if let Some(Slot::Ready(cached)) = shard.slots.get(&position.offset()) {
            cached.referenced.store(true, Ordering::Relaxed);
        }
        Some(body)
    }

    /// Evicts bodies until total bytes fit the budget, sweeping shards
    /// round-robin from `start_shard` and freeing in bulk (one gauge update
    /// per shard). The first pass is opportunistic (`try_write`); if the
    /// budget is still exceeded, a second blocking pass runs so sustained
    /// read pressure cannot starve eviction (a queued parking_lot writer
    /// blocks new readers, so `write_shard` acquires even under a stream of
    /// hits). After the blocking pass every shard has been drained as far
    /// as needed; any remaining excess belongs to in-flight fills, whose
    /// own sweeps run next.
    fn trim(&self, start_shard: usize) {
        for blocking in [false, true] {
            for i in 0..self.shards.len() {
                let over = self
                    .bytes
                    .load(Ordering::Relaxed)
                    .saturating_sub(self.max_bytes);
                if over == 0 {
                    return;
                }
                let lock = &self.shards[(start_shard + i) % self.shards.len()];
                let mut shard = if blocking {
                    self.write_shard(lock)
                } else {
                    let Some(shard) = lock.try_write() else {
                        continue;
                    };
                    shard
                };
                let (freed, evicted) = shard.evict_up_to(over);
                if evicted > 0 {
                    let _: usize = self.occupied.fetch_sub(evicted, Ordering::Relaxed);
                    self.sub_bytes(freed);
                    self.metrics.evictions.inc_by(evicted as u64);
                }
            }
        }
    }

    fn add_bytes(&self, len: usize) {
        let _: usize = self.bytes.fetch_add(len, Ordering::Relaxed);
        self.metrics.bytes.add(len as i64);
    }

    fn sub_bytes(&self, len: usize) {
        let _: usize = self.bytes.fetch_sub(len, Ordering::Relaxed);
        self.metrics.bytes.add(-(len as i64));
    }
}

impl Shard {
    /// Evicts bodies until at least `target` bytes are freed or the shard
    /// has none left; returns `(freed_bytes, evicted_count)`.
    fn evict_up_to(&mut self, target: usize) -> (usize, usize) {
        let mut freed = 0;
        let mut evicted = 0;
        while freed < target {
            match self.evict_one() {
                Some(len) => {
                    freed += len;
                    evicted += 1;
                }
                None => break,
            }
        }
        (freed, evicted)
    }

    /// Evicts one body via second chance: a `referenced` entry gets its bit
    /// cleared and one more trip around the ring; the first un-referenced
    /// entry is evicted. Returns the freed byte count, or `None` if the
    /// shard holds no `Ready` bodies. Terminates because the caller holds
    /// the shard write lock, so no reader can re-set a bit mid-walk: each
    /// entry is pushed back at most once before an un-referenced one pops.
    fn evict_one(&mut self) -> Option<usize> {
        loop {
            let offset = self.clock.pop_front()?;
            let Some(Slot::Ready(cached)) = self.slots.get(&offset) else {
                panic!("clock ring entry {offset} has no Ready slot");
            };
            if cached.referenced.swap(false, Ordering::Relaxed) {
                self.clock.push_back(offset);
            } else {
                let Some(Slot::Ready(cached)) = self.slots.remove(&offset) else {
                    unreachable!("checked above");
                };
                return Some(cached.body.len());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use std::sync::Barrier;

    fn cache_with_metrics(max_bytes: usize) -> (PositionCache, Arc<Metrics>) {
        let metrics = Metrics::new();
        let cache = PositionCache::new(
            max_bytes,
            Box::new(|_| true),
            metrics.decompress_cache_metrics(),
        );
        (cache, metrics)
    }

    fn cache(max_bytes: usize) -> PositionCache {
        cache_with_metrics(max_bytes).0
    }

    fn body(len: usize, fill: u8) -> Bytes {
        Bytes::from(vec![fill; len])
    }

    fn fill(cache: &PositionCache, pos: WalPosition, b: Bytes) -> CacheOutcome {
        cache.get_or_fill(pos, move || b)
    }

    /// Positions that map to the same shard, so eviction tests exercise a
    /// single clock ring deterministically.
    fn same_shard_positions(cache: &PositionCache, n: usize) -> Vec<WalPosition> {
        let target = cache.shard_index(WalPosition::test_value(0));
        (0..u64::MAX)
            .map(WalPosition::test_value)
            .filter(|p| cache.shard_index(*p) == target)
            .take(n)
            .collect()
    }

    /// A position guaranteed to hash to a different shard than `other`.
    fn position_in_other_shard(cache: &PositionCache, other: WalPosition) -> WalPosition {
        (0..u64::MAX)
            .map(WalPosition::test_value)
            .find(|p| cache.shard_index(*p) != cache.shard_index(other))
            .unwrap()
    }

    #[test]
    fn get_returns_cached_body() {
        let cache = cache(8 * 1024);
        let pos = WalPosition::test_value(7);
        assert!(cache.get(pos).is_none());
        assert!(matches!(
            fill(&cache, pos, body(100, 1)),
            CacheOutcome::Filled(_)
        ));
        assert_eq!(cache.get(pos).unwrap(), body(100, 1));
        assert_eq!(cache.peek(pos).unwrap(), body(100, 1));
        assert!(matches!(
            cache.get_or_fill(pos, || unreachable!("cached")),
            CacheOutcome::Hit(_)
        ));
    }

    #[test]
    fn remove_drops_body_and_reports_it() {
        let (cache, metrics) = cache_with_metrics(8 * 1024);
        let pos = WalPosition::test_value(5);
        assert!(!cache.remove(pos));
        let _: CacheOutcome = fill(&cache, pos, body(100, 1));
        assert_eq!(100, metrics.decompress_cache_bytes.get());
        assert!(cache.remove(pos));
        assert!(cache.get(pos).is_none());
        assert!(cache.is_empty());
        assert_eq!(0, metrics.decompress_cache_bytes.get());
        assert!(!cache.remove(pos));
    }

    #[test]
    fn peek_does_not_wait_on_in_flight_fill() {
        let cache = Arc::new(cache(8 * 1024));
        let pos = WalPosition::test_value(1);
        let in_fill = Arc::new(Barrier::new(2));
        let finish = Arc::new(Barrier::new(2));
        let leader = {
            let cache = cache.clone();
            let in_fill = in_fill.clone();
            let finish = finish.clone();
            std::thread::spawn(move || {
                let _: CacheOutcome = cache.get_or_fill(pos, move || {
                    in_fill.wait();
                    finish.wait();
                    body(100, 1)
                });
            })
        };
        in_fill.wait();
        // The fill is in flight: `get` would block here, `peek` must not.
        assert!(cache.peek(pos).is_none());
        finish.wait();
        leader.join().unwrap();
        assert_eq!(cache.peek(pos).unwrap(), body(100, 1));
    }

    #[test]
    fn second_chance_protects_referenced_entries() {
        // Budget fits two 400-byte bodies, not three.
        let (cache, metrics) = cache_with_metrics(1000);
        let pos = same_shard_positions(&cache, 3);
        let _: CacheOutcome = fill(&cache, pos[0], body(400, 0));
        let _: CacheOutcome = fill(&cache, pos[1], body(400, 1));
        assert_eq!(0, metrics.decompress_cache_evictions.get());
        // Touch pos[0]: its referenced bit protects it over pos[1].
        let _: Option<Bytes> = cache.get(pos[0]);
        let _: CacheOutcome = fill(&cache, pos[2], body(400, 2));
        assert!(cache.get(pos[1]).is_none());
        assert_eq!(cache.get(pos[0]).unwrap(), body(400, 0));
        assert_eq!(cache.get(pos[2]).unwrap(), body(400, 2));
        assert_eq!(1, metrics.decompress_cache_evictions.get());
    }

    #[test]
    fn remove_during_fill_retains_nothing() {
        let (cache, metrics) = cache_with_metrics(8 * 1024);
        let cache = Arc::new(cache);
        let pos = WalPosition::test_value(1);
        let in_fill = Arc::new(Barrier::new(2));
        let finish = Arc::new(Barrier::new(2));
        let leader = {
            let cache = cache.clone();
            let in_fill = in_fill.clone();
            let finish = finish.clone();
            std::thread::spawn(move || {
                cache.get_or_fill(pos, move || {
                    in_fill.wait();
                    finish.wait();
                    body(100, 1)
                })
            })
        };
        in_fill.wait();
        // Retired while the fill is in flight: nothing resident to drop yet.
        assert!(!cache.remove(pos));
        finish.wait();
        // The leader still gets its body, but the cache keeps nothing.
        assert_eq!(leader.join().unwrap().into_body(), body(100, 1));
        assert!(cache.peek(pos).is_none());
        assert!(cache.is_empty());
        assert_eq!(0, metrics.decompress_cache_bytes.get());
        // A later fill of the same position is retained as usual.
        let _: CacheOutcome = fill(&cache, pos, body(100, 2));
        assert_eq!(cache.peek(pos).unwrap(), body(100, 2));
    }

    #[test]
    fn oversized_body_returned_but_not_cached() {
        let (cache, metrics) = cache_with_metrics(1000);
        let pos = WalPosition::test_value(3);
        let outcome = fill(&cache, pos, body(1001, 0));
        assert_eq!(outcome.into_body(), body(1001, 0));
        assert!(cache.get(pos).is_none());
        assert!(cache.is_empty());
        assert_eq!(1, metrics.decompress_cache_rejected.get());
        assert_eq!(0, metrics.decompress_cache_bytes.get());
    }

    #[test]
    fn budget_is_global_across_shards() {
        let cache = cache(1000);
        for i in 0..8 {
            let _: CacheOutcome = fill(&cache, WalPosition::test_value(i), body(400, i as u8));
        }
        assert!(cache.bytes.load(Ordering::Relaxed) <= 1000);
    }

    #[test]
    fn remove_via_unreachable_keeps_ring_consistent() {
        let (cache, metrics) = {
            let metrics = Metrics::new();
            let reachable = Arc::new(AtomicBool::new(true));
            let flag = reachable.clone();
            let cache = PositionCache::new(
                1000,
                Box::new(move |_| flag.load(Ordering::Relaxed)),
                metrics.decompress_cache_metrics(),
            );
            (CacheAndFlag { cache, reachable }, metrics)
        };
        let pos = same_shard_positions(&cache.cache, 4);
        let _: CacheOutcome = fill(&cache.cache, pos[0], body(300, 0));
        let _: CacheOutcome = fill(&cache.cache, pos[1], body(300, 1));
        // Reclaim: every position reports unreachable; the next get drops
        // pos[0] instead of serving it.
        cache.reachable.store(false, Ordering::Relaxed);
        assert!(cache.cache.get(pos[0]).is_none());
        cache.reachable.store(true, Ordering::Relaxed);
        // The stale entry is gone even now that the flag is back.
        assert!(cache.cache.get(pos[0]).is_none());
        assert_eq!(cache.cache.get(pos[1]).unwrap(), body(300, 1));
        assert_eq!(300, cache.cache.bytes.load(Ordering::Relaxed));
        assert_eq!(300, metrics.decompress_cache_bytes.get());
        // Fill past the budget: eviction walks the clock ring, which must
        // have stayed consistent with the slots across the removal. The get
        // above referenced pos[1], so second chance protects it and the
        // un-referenced pos[2] is evicted instead.
        let _: CacheOutcome = fill(&cache.cache, pos[2], body(400, 2));
        let _: CacheOutcome = fill(&cache.cache, pos[3], body(400, 3));
        assert_eq!(cache.cache.get(pos[1]).unwrap(), body(300, 1));
        assert!(cache.cache.get(pos[2]).is_none());
        assert_eq!(cache.cache.get(pos[3]).unwrap(), body(400, 3));
        assert_eq!(700, cache.cache.bytes.load(Ordering::Relaxed));
    }

    struct CacheAndFlag {
        cache: PositionCache,
        reachable: Arc<AtomicBool>,
    }

    #[test]
    fn fill_settles_budget_within_shard() {
        let cache = cache(1000);
        let pos = same_shard_positions(&cache, 2);
        let _: CacheOutcome = fill(&cache, pos[0], body(900, 0));
        let _: CacheOutcome = fill(&cache, pos[1], body(900, 1));
        // The second fill's sweep wraps around to this shard and evicts the
        // older un-referenced body; the newcomer survives.
        assert!(cache.bytes.load(Ordering::Relaxed) <= 1000);
        assert!(cache.get(pos[0]).is_none());
        assert_eq!(cache.get(pos[1]).unwrap(), body(900, 1));
    }

    #[test]
    fn trim_settles_budget_across_shards() {
        let cache = cache(1000);
        let a = WalPosition::test_value(0);
        let b = position_in_other_shard(&cache, a);
        let _: CacheOutcome = fill(&cache, a, body(900, 0));
        let _: CacheOutcome = fill(&cache, b, body(900, 1));
        // The sweep starting after b's shard reaches a's shard and evicts
        // its cold body; the newcomer survives.
        assert!(cache.bytes.load(Ordering::Relaxed) <= 1000);
        assert!(cache.get(a).is_none());
        assert_eq!(cache.get(b).unwrap(), body(900, 1));
    }

    #[test]
    fn gauge_tracks_retained_bytes() {
        let (cache, metrics) = cache_with_metrics(8 * 1024);
        let _: CacheOutcome = fill(&cache, WalPosition::test_value(1), body(500, 1));
        assert_eq!(500, metrics.decompress_cache_bytes.get());
    }

    #[test]
    fn occupied_gate_covers_in_flight_fills() {
        let cache = Arc::new(cache(8 * 1024));
        assert!(cache.is_empty());
        let in_fill = Arc::new(Barrier::new(2));
        let finish = Arc::new(Barrier::new(2));
        let leader = {
            let cache = cache.clone();
            let in_fill = in_fill.clone();
            let finish = finish.clone();
            std::thread::spawn(move || {
                let _: CacheOutcome = cache.get_or_fill(WalPosition::test_value(1), move || {
                    in_fill.wait();
                    finish.wait();
                    body(100, 1)
                });
            })
        };
        in_fill.wait();
        // A fill is in flight: the gate must report non-empty so read paths
        // probe (and join the fill) instead of duplicating the frame read.
        assert!(!cache.is_empty());
        finish.wait();
        leader.join().unwrap();
        assert!(!cache.is_empty());
    }

    #[test]
    fn concurrent_misses_fill_once() {
        let cache = Arc::new(cache(1024 * 1024));
        let pos = WalPosition::test_value(11);
        let fills = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let fills = fills.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let outcome = cache.get_or_fill(pos, || {
                        let _: usize = fills.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        body(512, 7)
                    });
                    assert_eq!(outcome.into_body(), body(512, 7));
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(1, fills.load(Ordering::Relaxed));
    }

    #[test]
    fn waiters_recover_from_failed_leader() {
        let cache = Arc::new(cache(1024 * 1024));
        let pos = WalPosition::test_value(13);
        let leader_running = Arc::new(Barrier::new(2));
        let leader = {
            let cache = cache.clone();
            let leader_running = leader_running.clone();
            std::thread::spawn(move || {
                let _: CacheOutcome = cache.get_or_fill(pos, || {
                    leader_running.wait();
                    panic!("simulated corrupt frame");
                });
            })
        };
        leader_running.wait();
        // Whether this waits on the doomed leader (RetryRead: it released
        // its frame copy) or arrives after the cleanup (leads itself), it
        // must end up producing the body.
        let outcome = loop {
            match cache.get_or_fill(pos, || body(64, 9)) {
                CacheOutcome::RetryRead => continue,
                outcome => break outcome,
            }
        };
        assert_eq!(outcome.into_body(), body(64, 9));
        assert!(leader.join().is_err());
        assert_eq!(cache.get(pos).unwrap(), body(64, 9));
    }

    #[test]
    fn concurrent_access_is_safe() {
        let cache = Arc::new(cache(64 * 1024));
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for i in 0..1000u64 {
                        let pos = WalPosition::test_value(i % 32);
                        if let Some(hit) = cache.get(pos) {
                            assert_eq!(hit.len(), 512);
                        } else {
                            let _: CacheOutcome = cache.get_or_fill(pos, || body(512, t));
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
    }
}
