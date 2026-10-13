//! Cache of whole index blobs that have aged out of the index WAL's mmap
//! window.
//!
//! A point lookup or iterator step that misses the in-memory overlay walks
//! the cell's on-disk levels. Per level, the lookup-header format reads one
//! header element and then the key's **whole micro-cell section**. While the
//! blob's fragment is mapped the section is a zero-copy slice; once the
//! window has moved past it (`WalRandomRead::File`) the section is `pread`
//! into a fresh buffer on every lookup. For a normally sized cell that is a
//! few tens of KB. For a cell inflated by key skew — many keys, and every
//! retained version of each, sharing one micro-cell — the section is most
//! of a blob that can approach the fragment ceiling, and each lookup copies
//! it again: close to a gigabyte per lookup, and a few dozen lookups per
//! second are enough to saturate a node's CPU.
//!
//! This cache sits behind the mmap window: a blob is served zero-copy from
//! its mapped fragment while the fragment is young, and from here once the
//! window has moved past it. It is an instance of
//! [`PositionCache`](crate::position_cache) keyed by the blob's WAL position,
//! which is immutable within a `Db` instance (`write_index` never rewrites
//! in place; promotes, sharded promotes and relocation always produce new
//! positions). Whole blobs are cached rather than sections: a cached blob is
//! handed back through [`WalRandomRead::Cached`](crate::wal::WalRandomRead),
//! whose reads are slices, so every consumer (`read_micro_cell_section`,
//! `lookup_unloaded`, `next_entry_unloaded`, the iterator caches) works
//! unchanged and the header read disappears too.
//!
//! Admission is decided in `Db::index_reader`, by blob size, and only on the
//! syscall path: a `File` reader whose blob is at least
//! `Config::index_blob_cache_min_bytes` and at most the budget is wrapped in
//! an [`IndexBlobFill`]. Blob size is a usable proxy for "has an oversized
//! section" because a blob can only grow large through skew (a uniform key
//! space would need millions of keys per cell). A blob above the budget
//! could never be retained, so it keeps the plain reader rather than being
//! read whole on every lookup (`index_blob_cache_rejected` counts those
//! reader opens). Mapped blobs are never admitted, so a freshly written blob
//! (always in the writer's mapped fragment) enters the cache on the first
//! syscall-path lookup after it leaves the window, not at write time.
//!
//! Readers are opened under the row lock (so a concurrent relocation cannot
//! delete the blob out from under them), and the fill of a large blob takes
//! hundreds of milliseconds, so the fill must not run there. The probe under
//! the lock is non-blocking ([`IndexBlobCache::peek`]) and a miss yields an
//! [`IndexBlobFill`]: a lazy reader whose first `read` — after the row lock
//! is released, inside the lookup's `block_in_place` — runs the fill as
//! single-flight leader or joins the fill another lookup already started.
//! The leader reads the whole frame through `Wal::read`, which validates the
//! CRC, and keeps the payload after the index prefix: exactly the bytes the
//! plain reader serves, so every consumer sees identical data. A frame that
//! cannot be validated — its file reclaimed since the reader was opened, a
//! CRC mismatch, a read error — is served to the leader and its waiters as
//! the raw bytes the plain reader would have read, and is never retained:
//! lookups keep the uncached behaviour, while `Db::load` keeps validating
//! from disk and so can never adopt a corrupt blob from the cache.
//! `index_blob_cache_unreadable` counts the validation failures. The
//! leader's latency equals the plain section read plus one CRC pass;
//! waiters are bounded by the leader instead of each copying the blob
//! themselves.
//!
//! Eviction is the generic cache's CLOCK sweep by bytes, plus explicit drops:
//! the completion paths that replace a cell's levels (the flusher's
//! `Db::update_flushed_index` and `Db::update_relocated_index`, and both
//! callers of `sync_flush_for_relocation`) report the positions no level
//! references any more and [`remove`](IndexBlobCache::remove) each one, so
//! a superseded L1 does not sit in the budget for a whole eviction cycle.
//! Those positions are also remembered in a bounded retired set: a reader
//! built before the swap whose fill starts after the drop, or one that joins
//! after a retired fill has finished, bypasses the cache and reads its
//! sections from the file like the plain reader, instead of filling the
//! cache with a blob nothing references. Two level replacements do not
//! report: a synchronous flush on unload (`Config::sync_flush`) and
//! `LargeTableEntry::clear` (dropping cells); blobs they orphan are only
//! ever retained, never served, and the CLOCK sweep reclaims them.
//! Reachability (`Wal::is_reachable`) remains the backstop for blobs whose
//! file was garbage-collected. `Db::load` (used by promotes, relocation and
//! `maybe_load`) also serves from the cache when the blob is resident, so a
//! promote of the hot cell stops re-reading its L1 from disk, but it never
//! admits: only lookups fill the cache.
//!
//! Budget sizing: a cell's L0 and L1 can both qualify, fills in flight
//! overshoot the budget until their sweeps settle, and blobs orphaned by
//! the two non-reporting replacement paths are reclaimed only by the sweep.
//! Hence the headroom asked for in `Config::index_blob_cache_bytes`.
use crate::context::ReadType;
use crate::db::WalEntry;
use crate::lookup::{FileRange, RandomRead};
use crate::metrics::{MetricIntCounter, Metrics, TimerExt};
use crate::position_cache::{CacheOutcome, PositionCache, ReachabilityCheck};
use crate::wal::WalError;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};
use std::ops::Range;
use std::sync::{Arc, OnceLock};

/// Reads and CRC-validates the whole frame at a position, returning its
/// payload, or `None` if the frame's file has been reclaimed. Supplied by
/// `Db::open` as `Wal::read` on the index WAL; tests substitute stubs.
pub(crate) type FrameRead =
    Box<dyn Fn(WalPosition) -> Result<Option<Bytes>, WalError> + Send + Sync>;

/// How many retired positions are remembered. A completion retires a
/// handful of positions and a straggling reader resolves within
/// microseconds of the swap, so the set only has to span a short window.
/// An offset that has fallen out of the set reads as not retired, which
/// degrades to a retained but unreferenced blob that the CLOCK sweep
/// evicts.
const RETIRED_POSITIONS: usize = 4096;

pub(crate) struct IndexBlobCache {
    cache: PositionCache,
    /// Admission floor on blob size, `Config::index_blob_cache_min_bytes`.
    min_bytes: usize,
    read_frame: FrameRead,
    retired: Mutex<RetiredPositions>,
    hits: MetricIntCounter,
    fills: MetricIntCounter,
    fill_mcs: MetricIntCounter,
    drops: MetricIntCounter,
    rejected: MetricIntCounter,
    unreadable: MetricIntCounter,
}

/// FIFO-bounded set of WAL offsets no cell level references any more.
#[derive(Default)]
struct RetiredPositions {
    order: VecDeque<u64>,
    set: HashSet<u64>,
}

impl RetiredPositions {
    fn insert(&mut self, offset: u64) {
        if !self.set.insert(offset) {
            return;
        }
        self.order.push_back(offset);
        if self.order.len() > RETIRED_POSITIONS
            && let Some(oldest) = self.order.pop_front()
        {
            let _: bool = self.set.remove(&oldest);
        }
    }

    fn contains(&self, offset: u64) -> bool {
        self.set.contains(&offset)
    }
}

impl IndexBlobCache {
    pub fn new(
        max_bytes: usize,
        min_bytes: usize,
        reachable: ReachabilityCheck,
        read_frame: FrameRead,
        metrics: &Metrics,
    ) -> Self {
        Self {
            cache: PositionCache::new(max_bytes, reachable, metrics.index_blob_cache_metrics()),
            min_bytes,
            read_frame,
            retired: Mutex::default(),
            hits: metrics.index_blob_cache_hits.clone(),
            fills: metrics.index_blob_cache_fills.clone(),
            fill_mcs: metrics.index_blob_cache_fill_mcs.clone(),
            drops: metrics.index_blob_cache_drops.clone(),
            rejected: metrics.index_blob_cache_rejected.clone(),
            unreadable: metrics.index_blob_cache_unreadable.clone(),
        }
    }

    /// Non-blocking probe, safe under the row lock: returns the blob at
    /// `position` if it is resident and its frame is still reachable. An
    /// in-flight fill reads as a miss; the caller's [`IndexBlobFill`] joins
    /// it once the lock is released. Skips the shard lock entirely while
    /// the cache holds nothing, so databases where no blob ever qualifies
    /// pay one atomic load per reader.
    pub fn peek(&self, position: WalPosition) -> Option<Bytes> {
        if self.cache.is_empty() {
            return None;
        }
        let blob = self.cache.peek(position)?;
        self.hits.inc();
        Some(blob)
    }

    /// Whether a syscall-path blob of `blob_len` bytes (the frame payload
    /// after the index prefix, i.e. what the reader serves) qualifies for
    /// the cache: at least the floor, and no larger than the budget, since
    /// a blob the cache could never retain must not be read whole on every
    /// lookup. Counts each reader open above the budget as a rejection. The
    /// length comes from the frame header the reader was built from, so the
    /// test costs nothing.
    pub fn admit(&self, blob_len: usize) -> bool {
        if blob_len == 0 || blob_len < self.min_bytes {
            return false;
        }
        if blob_len > self.cache.max_bytes() {
            self.rejected.inc();
            return false;
        }
        true
    }

    /// Retires `position`: no cell level references it any more (a flush or
    /// relocation replaced it), so its blob is dropped if resident, a fill
    /// in flight for it is served to its waiters without being retained,
    /// and readers that resolve later bypass the cache.
    pub fn remove(&self, position: WalPosition) {
        self.retired.lock().insert(position.offset());
        if self.cache.is_empty() {
            return;
        }
        if self.cache.remove(position) {
            self.drops.inc();
        }
    }

    /// Wraps the `File` reader of an admitted blob in a lazy reader that
    /// fills the cache on first use. `file` must be the reader
    /// `Wal::random_reader_at` built for `position`, so the fill reads the
    /// same bytes the uncached path would serve.
    pub fn lazy_reader(self: &Arc<Self>, position: WalPosition, file: FileRange) -> IndexBlobFill {
        IndexBlobFill {
            cache: self.clone(),
            position,
            file,
            resolved: OnceLock::new(),
        }
    }

    /// Resolves the blob for an [`IndexBlobFill`]: bypassed if the position
    /// was retired since the reader was opened; otherwise cached, waited
    /// on, or read by this thread as the single-flight leader. A leader
    /// that panics (a read error, as `FileRange::read` does on the uncached
    /// path) leaves the waiters with `RetryRead`; they loop and lead their
    /// own fill.
    fn resolve(&self, position: WalPosition, file: &FileRange) -> Resolved {
        if self.retired.lock().contains(position.offset()) {
            return Resolved::Bypassed;
        }
        loop {
            let read = || {
                let _timer = self.fill_mcs.clone().mcs_timer();
                self.read_blob(position, file)
            };
            match self.cache.get_or_fill(position, read) {
                CacheOutcome::Hit(blob) => {
                    self.hits.inc();
                    return Resolved::Joined(blob);
                }
                CacheOutcome::Filled(blob) => {
                    self.fills.inc();
                    return Resolved::Led(blob);
                }
                CacheOutcome::RetryRead => {}
            }
        }
    }

    /// Reads the blob at `position` for a fill: the CRC-validated frame
    /// payload after the index prefix, which is byte for byte what `file`
    /// serves. If the frame cannot be validated — its file was reclaimed
    /// after the reader was opened, the CRC does not match, the read failed
    /// — the position is retired so nothing is retained, and the raw bytes
    /// are read through `file` exactly as the plain reader would read them
    /// (a read error then panics there, as it does on the uncached path).
    fn read_blob(&self, position: WalPosition, file: &FileRange) -> Bytes {
        match (self.read_frame)(position) {
            Ok(Some(payload)) => {
                let blob = payload.slice(WalEntry::INDEX_PREFIX_SIZE..);
                assert_eq!(
                    file.len(),
                    blob.len(),
                    "index blob at {position:?}: frame payload does not match the reader"
                );
                blob
            }
            Ok(None) => {
                self.remove(position);
                file.read(0..file.len())
            }
            Err(_) => {
                self.unreadable.inc();
                self.remove(position);
                file.read(0..file.len())
            }
        }
    }
}

/// How an [`IndexBlobFill`] serves its reads.
enum Resolved {
    /// From the blob this reader read as the single-flight leader.
    Led(Bytes),
    /// From the blob that was resident, or that another reader's fill
    /// supplied.
    Joined(Bytes),
    /// Straight from the file: the position was retired before this reader
    /// resolved.
    Bypassed,
}

/// A syscall-path index reader whose blob qualifies for the cache. Built
/// under the row lock from the `File` reader; the first `read` (after the
/// lock is released) fills the cache or joins an in-flight fill, and every
/// read is then a slice of the cached bytes. Holding the `FileRange` keeps
/// the file open for the fill, exactly as the plain `File` reader does for
/// its section reads.
pub struct IndexBlobFill {
    cache: Arc<IndexBlobCache>,
    position: WalPosition,
    file: FileRange,
    resolved: OnceLock<Resolved>,
}

impl IndexBlobFill {
    /// Blocks on another thread's in-flight fill of the same position, so
    /// it must not be called while holding a row lock.
    fn resolved(&self) -> &Resolved {
        self.resolved
            .get_or_init(|| self.cache.resolve(self.position, &self.file))
    }

    /// Reads `range` of the blob, resolving it on first use.
    pub fn read(&self, range: Range<usize>) -> Bytes {
        match self.resolved() {
            Resolved::Led(blob) | Resolved::Joined(blob) => blob.slice(range),
            Resolved::Bypassed => self.file.read(range),
        }
    }

    /// Blob length, known from the reader without resolving it.
    pub fn len(&self) -> usize {
        self.file.len()
    }

    /// How the blob was obtained, for `lookup_mcs{type}`: `Syscall` if this
    /// reader read it from the file, as the fill leader or because it
    /// bypassed the cache (the cost of a syscall-path lookup); `Cached` if
    /// it was resident or another thread's fill supplied it. Meaningful
    /// after the reads it labels; an unresolved reader reports `Cached`.
    pub fn read_type(&self) -> ReadType {
        match self.resolved.get() {
            Some(Resolved::Led(_) | Resolved::Bypassed) => ReadType::Syscall,
            Some(Resolved::Joined(_)) | None => ReadType::Cached,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc::CrcReadError;
    use crate::file_reader::FileReader;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const BLOB: &[u8] = &[7u8; 256];

    fn blob_file(dir: &Path) -> Arc<File> {
        let path = dir.join("blob");
        let mut file = File::create(&path).unwrap();
        file.write_all(BLOB).unwrap();
        Arc::new(File::open(&path).unwrap())
    }

    fn file_range(file: &Arc<File>) -> FileRange {
        FileRange::new(FileReader::new(file.clone(), false), 0..BLOB.len() as u64)
    }

    /// The whole index entry payload: the two-byte prefix (entry type,
    /// keyspace) followed by the blob.
    fn frame_payload() -> Bytes {
        let mut payload = vec![0xEE, 0x01];
        payload.extend_from_slice(BLOB);
        Bytes::from(payload)
    }

    fn read_all(reader: &IndexBlobFill) -> Bytes {
        reader.read(0..reader.len())
    }

    /// A cache whose frame read serves the validated payload, as `Wal::read`
    /// does in production.
    fn cache(max_bytes: usize, min_bytes: usize) -> (Arc<IndexBlobCache>, Arc<Metrics>) {
        cache_with(
            max_bytes,
            min_bytes,
            Box::new(|_| true),
            Box::new(|_| Ok(Some(frame_payload()))),
        )
    }

    fn cache_with(
        max_bytes: usize,
        min_bytes: usize,
        reachable: ReachabilityCheck,
        read_frame: FrameRead,
    ) -> (Arc<IndexBlobCache>, Arc<Metrics>) {
        let metrics = Metrics::new();
        let cache = IndexBlobCache::new(max_bytes, min_bytes, reachable, read_frame, &metrics);
        (Arc::new(cache), metrics)
    }

    /// A frame read that counts its calls and returns `result` each time.
    fn counting_frame_read(
        calls: Arc<AtomicUsize>,
        result: impl Fn() -> Result<Option<Bytes>, WalError> + Send + Sync + 'static,
    ) -> FrameRead {
        Box::new(move |_| {
            let _: usize = calls.fetch_add(1, Ordering::SeqCst);
            result()
        })
    }

    #[test]
    fn admission_floor_and_budget() {
        let (cache, metrics) = cache(1024, 100);
        assert!(!cache.admit(0));
        assert!(!cache.admit(99));
        assert!(cache.admit(100));
        assert!(cache.admit(1024));
        assert_eq!(0, metrics.index_blob_cache_rejected.get());
        // Larger than the budget: never admitted, counted as rejected.
        assert!(!cache.admit(1025));
        assert_eq!(1, metrics.index_blob_cache_rejected.get());
    }

    #[test]
    fn lazy_reader_fills_once_then_serves_from_cache() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let (cache, metrics) = cache(1024, 1);
        let pos = WalPosition::test_value(42);
        assert!(cache.peek(pos).is_none());

        let reader = cache.lazy_reader(pos, file_range(&file));
        assert_eq!(BLOB.len(), reader.len());
        assert_eq!(ReadType::Cached, reader.read_type());
        assert_eq!(BLOB, read_all(&reader).as_ref());
        // Resolved once: the second read returns the same bytes without a fill.
        assert_eq!(&BLOB[10..20], reader.read(10..20).as_ref());
        assert_eq!(ReadType::Syscall, reader.read_type());
        assert_eq!(1, metrics.index_blob_cache_fills.get());
        assert_eq!(0, metrics.index_blob_cache_hits.get());
        assert_eq!(BLOB.len() as i64, metrics.index_blob_cache_bytes.get());

        // Resident now: the lock-side probe hits ...
        assert_eq!(BLOB, cache.peek(pos).unwrap().as_ref());
        assert_eq!(1, metrics.index_blob_cache_hits.get());
        // ... and so does a lazy reader built from a miss that raced the fill.
        let reader = cache.lazy_reader(pos, file_range(&file));
        assert_eq!(BLOB, read_all(&reader).as_ref());
        assert_eq!(ReadType::Cached, reader.read_type());
        assert_eq!(1, metrics.index_blob_cache_fills.get());
        assert_eq!(2, metrics.index_blob_cache_hits.get());
    }

    #[test]
    fn fill_reads_the_file_when_the_frame_was_reclaimed() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let calls = Arc::new(AtomicUsize::new(0));
        let (cache, metrics) = cache_with(
            1024,
            1,
            Box::new(|_| true),
            counting_frame_read(calls.clone(), || Ok(None)),
        );
        let pos = WalPosition::test_value(42);
        // The reader's own descriptor serves the bytes; nothing is retained.
        let reader = cache.lazy_reader(pos, file_range(&file));
        assert_eq!(BLOB, read_all(&reader).as_ref());
        assert_eq!(ReadType::Syscall, reader.read_type());
        assert_eq!(0, metrics.index_blob_cache_bytes.get());
        assert_eq!(0, metrics.index_blob_cache_unreadable.get());
        assert!(cache.peek(pos).is_none());
        // Later readers of the retired position do not try again.
        let reader = cache.lazy_reader(pos, file_range(&file));
        assert_eq!(BLOB, read_all(&reader).as_ref());
        assert_eq!(ReadType::Syscall, reader.read_type());
        assert_eq!(1, calls.load(Ordering::SeqCst));
    }

    #[test]
    fn fill_bypasses_the_cache_on_an_unreadable_frame() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let calls = Arc::new(AtomicUsize::new(0));
        let (cache, metrics) = cache_with(
            1024,
            1,
            Box::new(|_| true),
            counting_frame_read(calls.clone(), || {
                Err(WalError::Crc(CrcReadError::CrcMismatch))
            }),
        );
        let pos = WalPosition::test_value(42);
        // The raw bytes the plain reader would serve, never retained.
        let reader = cache.lazy_reader(pos, file_range(&file));
        assert_eq!(BLOB, read_all(&reader).as_ref());
        assert_eq!(ReadType::Syscall, reader.read_type());
        assert_eq!(1, metrics.index_blob_cache_unreadable.get());
        assert_eq!(0, metrics.index_blob_cache_bytes.get());
        assert_eq!(0, metrics.index_blob_cache_drops.get());
        assert!(cache.peek(pos).is_none());
        // Later readers bypass without another frame read.
        let reader = cache.lazy_reader(pos, file_range(&file));
        assert_eq!(&BLOB[..16], reader.read(0..16).as_ref());
        assert_eq!(ReadType::Syscall, reader.read_type());
        assert_eq!(1, calls.load(Ordering::SeqCst));
        assert_eq!(1, metrics.index_blob_cache_unreadable.get());
    }

    #[test]
    #[should_panic(expected = "does not match the reader")]
    fn fill_panics_when_the_frame_does_not_match_the_reader() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let (cache, _) = cache_with(
            1024,
            1,
            Box::new(|_| true),
            Box::new(|_| Ok(Some(Bytes::from(vec![0xEE, 0x01, 1, 2, 3])))),
        );
        let reader = cache.lazy_reader(WalPosition::test_value(42), file_range(&file));
        let _: Bytes = read_all(&reader);
    }

    #[test]
    fn remove_drops_resident_blob() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let (cache, metrics) = cache(1024, 1);
        let pos = WalPosition::test_value(42);
        let _: Bytes = read_all(&cache.lazy_reader(pos, file_range(&file)));
        assert_eq!(BLOB.len() as i64, metrics.index_blob_cache_bytes.get());
        cache.remove(pos);
        assert_eq!(1, metrics.index_blob_cache_drops.get());
        assert_eq!(0, metrics.index_blob_cache_bytes.get());
        assert!(cache.peek(pos).is_none());
    }

    #[test]
    fn remove_during_fill_serves_the_leader_but_retains_nothing() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let in_fill = Arc::new(Barrier::new(2));
        let finish = Arc::new(Barrier::new(2));
        let (cache, metrics) = {
            let in_fill = in_fill.clone();
            let finish = finish.clone();
            cache_with(
                1024,
                1,
                Box::new(|_| true),
                Box::new(move |_| {
                    in_fill.wait();
                    finish.wait();
                    Ok(Some(frame_payload()))
                }),
            )
        };
        let pos = WalPosition::test_value(42);
        let leader = {
            let reader = cache.lazy_reader(pos, file_range(&file));
            std::thread::spawn(move || (read_all(&reader), reader.read_type()))
        };
        in_fill.wait();
        // Retired while the fill is in flight.
        cache.remove(pos);
        finish.wait();
        let (blob, read_type) = leader.join().unwrap();
        assert_eq!(BLOB, blob.as_ref());
        assert_eq!(ReadType::Syscall, read_type);
        assert_eq!(1, metrics.index_blob_cache_fills.get());
        assert_eq!(0, metrics.index_blob_cache_drops.get());
        assert_eq!(0, metrics.index_blob_cache_bytes.get());
        assert!(cache.peek(pos).is_none());
    }

    #[test]
    fn remove_before_fill_bypasses_the_cache() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let calls = Arc::new(AtomicUsize::new(0));
        let (cache, metrics) = cache_with(
            1024,
            1,
            Box::new(|_| true),
            counting_frame_read(calls.clone(), || Ok(Some(frame_payload()))),
        );
        let pos = WalPosition::test_value(42);
        // A reader built before the level swap, resolving after the drop.
        let reader = cache.lazy_reader(pos, file_range(&file));
        cache.remove(pos);
        assert_eq!(BLOB, read_all(&reader).as_ref());
        assert_eq!(ReadType::Syscall, reader.read_type());
        assert_eq!(0, calls.load(Ordering::SeqCst));
        assert_eq!(0, metrics.index_blob_cache_fills.get());
        assert_eq!(0, metrics.index_blob_cache_bytes.get());
        assert!(cache.peek(pos).is_none());
    }

    #[test]
    fn peek_misses_unreachable_blob() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let reachable = Arc::new(AtomicBool::new(true));
        let flag = reachable.clone();
        let (cache, metrics) = cache_with(
            1024,
            1,
            Box::new(move |_| flag.load(Ordering::Relaxed)),
            Box::new(|_| Ok(Some(frame_payload()))),
        );
        let pos = WalPosition::test_value(42);
        let _: Bytes = read_all(&cache.lazy_reader(pos, file_range(&file)));
        assert!(cache.peek(pos).is_some());
        reachable.store(false, Ordering::Relaxed);
        assert!(cache.peek(pos).is_none());
        assert_eq!(0, metrics.index_blob_cache_bytes.get());
    }

    #[test]
    fn concurrent_lazy_readers_fill_once() {
        let dir = tempdir::TempDir::new("index-blob-cache").unwrap();
        let file = blob_file(dir.path());
        let (cache, metrics) = cache(1024, 1);
        let pos = WalPosition::test_value(42);
        const THREADS: usize = 8;
        let barrier = Arc::new(Barrier::new(THREADS));
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let reader = cache.lazy_reader(pos, file_range(&file));
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    assert_eq!(BLOB, read_all(&reader).as_ref());
                    reader.read_type()
                })
            })
            .collect();
        let leaders = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|read_type| *read_type == ReadType::Syscall)
            .count();
        assert_eq!(1, leaders);
        assert_eq!(1, metrics.index_blob_cache_fills.get());
        assert_eq!((THREADS - 1) as u64, metrics.index_blob_cache_hits.get());
    }
}
