// See docs/snapshot_mechanism.md for an overview of how the flusher integrates
// with the two-pass snapshot mechanism.
use crate::cell::CellId;
use crate::context::KsContext;
use crate::db::Db;
use crate::index::index_format::IndexFormat;
use crate::index::index_table::IndexTable;
use crate::index::levels::{IndexLevels, IndexShard};
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::Loader;
use crate::metrics::Metrics;
use crate::relocation::updates::RelocationUpdates;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

pub struct IndexFlusher {
    senders: Vec<mpsc::Sender<FlusherCommand>>,
    metrics: Arc<Metrics>,
}

pub(crate) struct IndexFlusherThread {
    db: Weak<Db>,
    receiver: mpsc::Receiver<FlusherCommand>,
    metrics: Arc<Metrics>,
    thread_id: usize,
}

pub enum FlusherCommand {
    Command(IndexFlushCommand),
    Barrier(Arc<SendGuard>),
}

pub struct IndexFlushCommand {
    pub(crate) ks: KeySpace,
    pub(crate) cell: CellId,
    pub(crate) flush_kind: FlushKind,
}

impl IndexFlushCommand {
    pub fn new(ks: KeySpace, cell: CellId, flush_kind: FlushKind) -> Self {
        Self {
            ks,
            cell,
            flush_kind,
        }
    }
}

pub enum FlushKind {
    /// Flush dirty contents, producing a new on-disk level state.
    ///
    /// - `dirty` is the dirty/loaded in-memory index provided by the caller.
    /// - `current_levels` is the entry's on-disk level state at the time the
    ///   flush was scheduled. It tells the flusher which L0 (if any) to
    ///   load-and-merge and which L1 (if any) to consult on promote.
    /// - `loaded` distinguishes the two caller shapes:
    ///   - `loaded = true` (was `FlushLoaded`): `dirty` already reflects
    ///     L0 + overlay — no separate L0 load is needed.
    ///   - `loaded = false` (was `MergeUnloaded`): `dirty` is only the
    ///     overlay; the flusher must load L0 from `current_levels.l0()`
    ///     (if any) and merge.
    Flush {
        dirty: Arc<IndexTable>,
        current_levels: IndexLevels,
        loaded: bool,
    },
    /// Re-write a clean entry's on-disk blobs past `force_relocate_below`.
    ///
    /// The flusher collapses all populated levels into a single new blob
    /// (L0 wins on overlap). The output is always single-level; callers
    /// that care about level-specific rewriting must add a richer variant
    /// — today collapse-on-relocate trades a one-time WAF penalty (L1
    /// rewritten even when only L0 was below cutoff) for a simpler code
    /// path and a reduced level count afterwards.
    ForceRelocate(IndexLevels),
}

impl IndexFlusher {
    pub fn new(senders: Vec<mpsc::Sender<FlusherCommand>>, metrics: Arc<Metrics>) -> Self {
        assert!(!senders.is_empty(), "Must have at least one flusher thread");
        Self { senders, metrics }
    }

    /// Start flusher threads with the given receivers and database reference
    pub fn start_threads(
        receivers: Vec<mpsc::Receiver<FlusherCommand>>,
        db: Weak<Db>,
        metrics: Arc<Metrics>,
    ) -> Vec<JoinHandle<()>> {
        receivers
            .into_iter()
            .enumerate()
            .map(|(thread_id, receiver)| {
                let flusher_thread =
                    IndexFlusherThread::new(db.clone(), receiver, metrics.clone(), thread_id);

                thread::Builder::new()
                    .name(format!("flusher-{thread_id}"))
                    .spawn(move || flusher_thread.run())
                    .unwrap()
            })
            .collect()
    }

    pub fn request_flush(&self, ks: KeySpace, cell: CellId, flush_kind: FlushKind) {
        let thread_index = self.get_thread_for_cell(&cell);
        let command = IndexFlushCommand::new(ks, cell, flush_kind);
        self.metrics.flush_pending.add(1);
        self.senders[thread_index]
            .send(FlusherCommand::Command(command))
            .expect("Flusher has stopped unexpectedly")
    }

    fn get_thread_for_cell(&self, cell: &CellId) -> usize {
        cell.mutex_seed() % self.senders.len()
    }

    #[cfg(test)]
    pub fn new_unstarted_for_test() -> Self {
        let (sender, _receiver) = mpsc::channel();
        Self::new(vec![sender], Metrics::new())
    }

    /// Wait until all messages that are currently queued for all flusher threads are processed.
    pub fn barrier(&self) {
        let mutex = Arc::new(parking_lot::Mutex::new(()));
        let guard = Arc::new(SendGuard(mutex.lock_arc()));
        for sender in &self.senders {
            self.metrics.flush_pending.add(1);
            sender
                .send(FlusherCommand::Barrier(Arc::clone(&guard)))
                .expect("Flusher has stopped unexpectedly");
        }
        drop(guard);
        drop(mutex.lock());
    }
}

impl IndexFlusherThread {
    pub fn new(
        db: Weak<Db>,
        receiver: mpsc::Receiver<FlusherCommand>,
        metrics: Arc<Metrics>,
        thread_id: usize,
    ) -> Self {
        Self {
            db,
            receiver,
            metrics,
            thread_id,
        }
    }

    pub fn run(self) {
        while let Ok(command) = self.receiver.recv() {
            match command {
                FlusherCommand::Barrier(_guard) => {
                    self.metrics.flush_pending.add(-1);
                    // Dropping _guard releases the mutex, allowing the barrier() caller to proceed
                }
                FlusherCommand::Command(command) => {
                    self.metrics.flush_pending.add(-1);
                    let now = Instant::now();
                    let Some(db) = self.db.upgrade() else {
                        return;
                    };

                    let ks_context = db.ks_context(command.ks);
                    let is_relocation = matches!(command.flush_kind, FlushKind::ForceRelocate(_));
                    if let Some((original_index, new_levels)) =
                        Self::handle_command(&*db, &command, ks_context, None, None)
                    {
                        if is_relocation {
                            db.update_relocated_index(command.ks, command.cell, new_levels);
                        } else {
                            db.update_flushed_index(
                                command.ks,
                                command.cell,
                                original_index,
                                new_levels,
                            );
                        }
                    }

                    self.metrics
                        .flush_time_mcs
                        .with_label_values(&[&self.thread_id.to_string()])
                        .inc_by(now.elapsed().as_micros() as u64);
                }
            }
        }
    }

    pub(crate) fn handle_command<L: Loader>(
        loader: &L,
        command: &IndexFlushCommand,
        ctx: &KsContext,
        relocation_updates: Option<RelocationUpdates>,
        relocation_cutoff: Option<u64>,
    ) -> Option<(Arc<IndexTable>, IndexLevels)> {
        // Build the merged L0 candidate plus the existing L1 (if any).
        //
        // For `Flush`, `current_levels` describes the cell's pre-flush on-disk
        // state; we honour the sentinel (empty L0 slot after a prior promote)
        // so post-promote cells start a fresh L0 with just the overlay.
        //
        // `ForceRelocate` collapses L0 + L1 into a single blob so the output
        // is always single-level (`existing_l1 = None`). Downstream this means
        // the non-promote branch will `clean_self` (since no L1 sits below)
        // and emit `IndexLevels::single(new_pos)`.
        let (original_index, mut merged_l0, existing_l1) = match &command.flush_kind {
            FlushKind::Flush {
                dirty,
                current_levels,
                loaded,
            } => {
                let label = if *loaded { "flush" } else { "merge_flush" };
                ctx.metrics.unload.with_label_values(&[label]).inc();
                let merged = if *loaded {
                    // `dirty` already reflects L0+overlay. Clone so we can
                    // mutate (compact, strip tombstones, etc.) without
                    // touching the shared in-memory table. Tombstone handling
                    // is deferred to the per-level branch below: if this
                    // flush produces the deepest blob we will `clean_self`
                    // there; otherwise tombstones must survive to shadow L1.
                    // todo - no need to make copy if there is no compactor
                    IndexTable::clone(dirty)
                } else {
                    let mut base = match current_levels.l0() {
                        Some(pos) => loader
                            .load(&ctx.ks_config, pos)
                            .expect("Failed to load L0 index in flusher thread"),
                        None => IndexTable::default(),
                    };
                    base.merge_dirty_and_clean(dirty);
                    base
                };
                (dirty.clone(), merged, current_levels.l1())
            }
            FlushKind::ForceRelocate(levels) => {
                // Collapse L0 + L1 into a single blob. Start from L1 (the
                // deeper level) and merge L0 on top so L0 wins on overlap,
                // matching the read-path precedence. The result is handed
                // downstream as `merged_l0` with `existing_l1 = None`, so
                // the non-promote branch writes it as a single level and
                // strips tombstones (since nothing sits below it).
                //
                // Force-relocate runs on clean entries only (see
                // `request_async_snapshot_flush`), so there is no dirty
                // overlay to worry about here.
                ctx.metrics
                    .unload
                    .with_label_values(&["force_relocate"])
                    .inc();
                let mut merged = Self::load_l1_or_default(loader, ctx, levels.l1());
                if let Some(l0_pos) = levels.l0() {
                    let l0 = loader
                        .load(&ctx.ks_config, l0_pos)
                        .expect("Failed to load L0 for forced relocation");
                    merged.merge_dirty_and_clean(&l0);
                }
                let arc_index = Arc::new(merged);
                (arc_index.clone(), IndexTable::clone(&arc_index), None)
            }
        };

        // `RelocationUpdates::apply` uses compare-and-set semantics — it can
        // only rewrite positions for keys already in `merged_l0`. Keys that
        // live only in L1 (the common case for post-promote `[INVALID, L1]`
        // cells) would be silently dropped. Pre-load L1 into `merged_l0` so
        // apply sees the full key set. The subsequent promote branch still
        // re-loads L1 from disk; the duplication is acceptable on the
        // relocation path — correctness first, optimize later.
        if relocation_updates.is_some() && existing_l1.is_some() {
            let mut combined = Self::load_l1_or_default(loader, ctx, existing_l1);
            // `merged_l0` is the fresher overlay (L0 + dirty) — it wins on
            // overlapping keys, so treat it as the "dirty" side.
            combined.merge_dirty_and_clean(&merged_l0);
            merged_l0 = combined;
        }
        if let Some(relocation_updates) = relocation_updates {
            relocation_updates.apply(&mut merged_l0);
        }

        match relocation_cutoff {
            Some(cutoff) => {
                let length = merged_l0.len();
                merged_l0.retain_above_position(cutoff);
                if merged_l0.len() == length {
                    return None;
                }
            }
            // TODO: Used only if relocation doesn't call sync flush. Remove if no such implementation is needed anymore
            None => merged_l0.retain_above_position(loader.min_wal_position()),
        }

        // Decide: write as a new L0 on top of the existing L1, or promote by
        // merging L0 into L1. `ForceRelocate` keeps single-level semantics.
        //
        // The read path walks `entry.levels()` — `get()` does a miss-chain
        // across levels and `next_in_cell` does a k-way merge — so
        // producing `[L0, L1]` is safe.
        let is_relocation = matches!(&command.flush_kind, FlushKind::ForceRelocate(_));
        let l0_threshold = ctx.l0_max_entries();
        let over_threshold = !is_relocation && merged_l0.len() > l0_threshold;

        let new_levels = if over_threshold {
            // Promote: merge the freshly-built L0 with the existing L1 (if
            // any). `merge_dirty_and_clean` lets `merged_l0` win on overlap
            // by treating it as the "dirty" overlay.
            let mut new_l1 = Self::load_l1_or_default(loader, ctx, existing_l1);
            new_l1.merge_dirty_and_clean(&merged_l0);
            Self::run_compactor(ctx, &mut new_l1);
            // L1 is the deepest level — nothing below to shadow, so drop
            // tombstones (and the keys they shadow) before writing.
            new_l1.clean_self();
            ctx.metrics
                .promote_total
                .with_label_values(&[ctx.name()])
                .inc();
            Self::write_promoted_l1(loader, ctx, command.ks, new_l1)
        } else {
            Self::run_compactor(ctx, &mut merged_l0);
            // If no L1 exists below, this L0 is the deepest level — strip
            // tombstones. Otherwise leave them in so they shadow L1.
            if existing_l1.is_none() {
                merged_l0.clean_self();
            }
            let new_l0_pos = loader
                .flush(command.ks, &merged_l0)
                .expect("Failed to flush index");
            if !is_relocation {
                ctx.metrics
                    .l0_bytes_written
                    .with_label_values(&[ctx.name()])
                    .inc_by(new_l0_pos.frame_len() as u64);
            }
            let mut levels = IndexLevels::single(new_l0_pos);
            if let Some(l1) = existing_l1 {
                levels.set(1, l1);
            }
            levels
        };

        Some((original_index, new_levels))
    }

    /// Load the L1 blob at `l1`, or return an empty `IndexTable` when `l1` is
    /// `None`. The `expect` here matches the other flusher `load` call sites —
    /// a failure indicates a corrupt or missing blob and isn't recoverable on
    /// this thread.
    fn load_l1_or_default<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        l1: Option<WalPosition>,
    ) -> IndexTable {
        match l1 {
            Some(pos) => loader
                .load(&ctx.ks_config, pos)
                .expect("Failed to load L1 index in flusher thread"),
            None => IndexTable::default(),
        }
    }

    // todo - result of compactor is not applied to in-memory index for DirtyLoaded
    fn run_compactor(ctx: &KsContext, index: &mut IndexTable) {
        if let Some(compactor) = ctx.ks_config.compactor() {
            let pre_compact_len = index.len();
            index.compact_with(|iter| (compactor)(iter));
            let compacted = pre_compact_len.saturating_sub(index.len());
            ctx.metrics
                .compacted_keys
                .with_label_values(&[ctx.name()])
                .inc_by(compacted as u64);
        }
    }

    /// Writes the freshly-promoted L1 image. When `auto_sharding` is on
    /// and the serialized blob exceeds the shard-split threshold, splits
    /// in-memory and flushes one blob per shard. The split is a pure
    /// in-memory partition of `new_l1` — no further shards are loaded
    /// from disk.
    fn write_promoted_l1<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        ks: KeySpace,
        new_l1: IndexTable,
    ) -> IndexLevels {
        let split_threshold = ctx.config.l1_shard_split_threshold();
        if ctx.config.auto_sharding {
            let serialized = ctx
                .ks_config
                .index_format()
                .serialize_index(&new_l1, &ctx.ks_config);
            if serialized.len() > split_threshold {
                return Self::flush_split_l1(loader, ctx, ks, new_l1, split_threshold);
            }
        }
        let pos = loader.flush(ks, &new_l1).expect("Failed to flush index");
        ctx.metrics
            .l1_bytes_written
            .with_label_values(&[ctx.name()])
            .inc_by(pos.frame_len() as u64);
        IndexLevels::promoted(pos)
    }

    /// Median-by-index splits `new_l1` until every shard's serialized
    /// size is ≤ `size_limit`, then flushes each as its own WAL blob.
    /// A shard that's still oversize after collapsing to a single entry
    /// is written as-is — the WAL layer will panic if the frame exceeds
    /// the fragment, matching the design's "reshape the keyspace" guidance.
    fn flush_split_l1<L: Loader>(
        loader: &L,
        ctx: &KsContext,
        ks: KeySpace,
        new_l1: IndexTable,
        size_limit: usize,
    ) -> IndexLevels {
        let entries: Vec<(Bytes, WalPosition)> = new_l1.iter().collect();
        let mut shard_tables: Vec<(Vec<u8>, Vec<u8>, IndexTable)> = Vec::new();
        Self::split_recursive(&entries, &ctx.ks_config, size_limit, &mut shard_tables);
        debug_assert!(
            !shard_tables.is_empty(),
            "split_recursive returned no shards for a non-empty L1",
        );

        let mut shards: BTreeMap<Vec<u8>, IndexShard> = BTreeMap::new();
        let mut total_bytes: u64 = 0;
        for (min_key, max_key, shard_table) in shard_tables.iter() {
            let pos = loader
                .flush(ks, shard_table)
                .expect("Failed to flush shard");
            total_bytes += pos.frame_len() as u64;
            shards.insert(min_key.clone(), IndexShard::new(pos, max_key.clone()));
        }
        ctx.metrics
            .l1_bytes_written
            .with_label_values(&[ctx.name()])
            .inc_by(total_bytes);
        ctx.metrics
            .l1_shards_total
            .with_label_values(&[ctx.name()])
            .inc_by(shards.len() as u64);
        ctx.metrics
            .l1_shard_split_total
            .with_label_values(&[ctx.name()])
            .inc();
        IndexLevels::sharded(shards)
    }

    /// Recursively bisect `entries` by index. Appends each chunk that
    /// fits `size_limit` (or has a single entry, which is always
    /// accepted) to `out`.
    fn split_recursive(
        entries: &[(Bytes, WalPosition)],
        ks: &KeySpaceDesc,
        size_limit: usize,
        out: &mut Vec<(Vec<u8>, Vec<u8>, IndexTable)>,
    ) {
        if entries.is_empty() {
            return;
        }
        let table = Self::build_shard_table(entries);
        let serialized = ks.index_format().serialize_index(&table, ks);
        if serialized.len() <= size_limit || entries.len() == 1 {
            let min_key = entries.first().unwrap().0.to_vec();
            let max_key = entries.last().unwrap().0.to_vec();
            out.push((min_key, max_key, table));
            return;
        }
        let mid = entries.len() / 2;
        Self::split_recursive(&entries[..mid], ks, size_limit, out);
        Self::split_recursive(&entries[mid..], ks, size_limit, out);
    }

    fn build_shard_table(entries: &[(Bytes, WalPosition)]) -> IndexTable {
        let mut table = IndexTable::default();
        for (k, v) in entries {
            table.insert(k.clone(), *v);
        }
        table
    }
}

pub struct SendGuard(#[allow(dead_code)] parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()>);

// Rather than enable send_guard feature globally in parking_lot,
// using this just for flusher barrier
unsafe impl Send for SendGuard {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::index::index_format::IndexFormat;
    use crate::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
    use crate::wal::WalRandomRead;
    use crate::wal::position::{LastProcessed, WalPosition};
    use minibytes::Bytes;
    use parking_lot::Mutex;
    use std::io;

    /// In-memory `Loader` that records each `flush` call's table length
    /// and hands out monotonically increasing fake `WalPosition`s.
    struct RecordingLoader {
        flushes: Mutex<Vec<usize>>, // entry count of each flushed table
        next_offset: Mutex<u64>,
    }

    impl RecordingLoader {
        fn new() -> Self {
            Self {
                flushes: Mutex::new(Vec::new()),
                next_offset: Mutex::new(1000),
            }
        }

        fn flush_count(&self) -> usize {
            self.flushes.lock().len()
        }

        fn flush_sizes(&self) -> Vec<usize> {
            self.flushes.lock().clone()
        }
    }

    impl Loader for RecordingLoader {
        type Error = io::Error;

        fn load(
            &self,
            _ks: &crate::key_shape::KeySpaceDesc,
            _position: WalPosition,
        ) -> Result<IndexTable, Self::Error> {
            Ok(IndexTable::default())
        }

        fn index_reader(&self, _position: WalPosition) -> Result<WalRandomRead, Self::Error> {
            unreachable!("RecordingLoader does not serve index reads")
        }

        fn flush_supported(&self) -> bool {
            true
        }

        fn flush(&self, _ks: KeySpace, data: &IndexTable) -> Result<WalPosition, Self::Error> {
            self.flushes.lock().push(data.len());
            let mut off = self.next_offset.lock();
            let pos = WalPosition::new(*off, 64);
            *off += 4096;
            Ok(pos)
        }

        fn last_processed_wal_position(&self) -> LastProcessed {
            LastProcessed::none()
        }

        fn current_wal_position(&self) -> u64 {
            1
        }

        fn min_wal_position(&self) -> u64 {
            0
        }
    }

    fn make_ctx(auto_sharding: bool, frag_size: u64) -> KsContext {
        let mut config = Config::small();
        config.frag_size = frag_size;
        config.auto_sharding = auto_sharding;
        config.l0_max_entries = Some(64);
        let (shape, ks_id) =
            KeyShape::new_single_config(8, 1, KeyType::uniform(8), KeySpaceConfig::default());
        let ks = shape.ks(ks_id).clone();
        KsContext::new(Arc::new(config), ks, Metrics::new())
    }

    fn build_table(n: usize) -> IndexTable {
        let mut t = IndexTable::default();
        for i in 0..n as u64 {
            let key = (i + 1).to_be_bytes().to_vec(); // 8-byte BE, all unique
            t.insert(Bytes::from(key), WalPosition::test_value(i + 1));
        }
        t
    }

    fn flush_command(ks: KeySpace, dirty: IndexTable, current: IndexLevels) -> IndexFlushCommand {
        IndexFlushCommand::new(
            ks,
            CellId::Integer(0),
            FlushKind::Flush {
                dirty: Arc::new(dirty),
                current_levels: current,
                loaded: true,
            },
        )
    }

    fn serialized_size(ks: &crate::key_shape::KeySpaceDesc, t: &IndexTable) -> usize {
        ks.index_format().serialize_index(t, ks).len()
    }

    #[test]
    fn auto_sharding_off_writes_single_l1_even_when_huge() {
        let ctx = make_ctx(false, 4096);
        let loader = RecordingLoader::new();
        // Far above l0_max_entries=64 and the synthetic 2 KiB threshold a
        // 4 KiB frag_size would imply — but auto_sharding is off, so the
        // flusher must keep today's single-blob shape.
        let dirty = build_table(1000);
        let cmd = flush_command(ctx.id(), dirty, IndexLevels::new());
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(!new_levels.is_sharded());
        assert_eq!(new_levels.l0(), None);
        assert!(new_levels.l1().is_some(), "expected promoted [INVALID, L1]");
        assert_eq!(loader.flush_count(), 1);
    }

    #[test]
    fn auto_sharding_on_small_l1_stays_unsharded() {
        // L1 fits in one fragment → no split, identical to the
        // auto_sharding=false shape.
        let ctx = make_ctx(true, 1024 * 1024);
        let loader = RecordingLoader::new();
        let dirty = build_table(200);
        let cmd = flush_command(ctx.id(), dirty, IndexLevels::new());
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(!new_levels.is_sharded());
        assert!(new_levels.l1().is_some());
        assert_eq!(loader.flush_count(), 1);
    }

    #[test]
    fn auto_sharding_on_large_l1_splits_into_multiple_shards() {
        // Tiny frag_size forces the split threshold below the serialized
        // size of the merged_l0, so the flusher must shard.
        let ctx = make_ctx(true, 4096);
        let loader = RecordingLoader::new();
        let dirty = build_table(1000);
        // Sanity: serialized size > the shard-split budget.
        assert!(serialized_size(&ctx.ks_config, &dirty) > ctx.config.l1_shard_split_threshold());

        let cmd = flush_command(ctx.id(), dirty, IndexLevels::new());
        let (_orig, new_levels) =
            IndexFlusherThread::handle_command(&loader, &cmd, &ctx, None, None).unwrap();
        assert!(new_levels.is_sharded(), "expected sharded IndexLevels");
        assert_eq!(
            new_levels.l1(),
            None,
            "L1 slot must be vacated when sharded"
        );
        assert!(new_levels.shards().len() > 1, "expected at least 2 shards");
        // Flush count matches the shard count exactly — one WAL blob per
        // shard, no extra writes.
        assert_eq!(loader.flush_count(), new_levels.shards().len());
        // Flushed entry counts add up to the original L1 size.
        assert_eq!(loader.flush_sizes().iter().sum::<usize>(), 1000);

        // Shards are disjoint and totally cover the input keyset.
        let shards: Vec<(&Vec<u8>, &IndexShard)> = new_levels.shards().iter().collect();
        let mut prev_max: Option<&[u8]> = None;
        for (min_key, shard) in &shards {
            assert!(min_key.as_slice() <= shard.max_key.as_slice());
            if let Some(prev) = prev_max {
                assert!(prev < min_key.as_slice(), "shards must be disjoint");
            }
            prev_max = Some(shard.max_key.as_slice());
        }
        assert_eq!(shards.first().unwrap().0, &1_u64.to_be_bytes().to_vec());
        assert_eq!(
            shards.last().unwrap().1.max_key,
            1000_u64.to_be_bytes().to_vec(),
        );

        // Re-run split_recursive directly to confirm every shard fits
        // the budget — the on-disk shape the WAL layer accepts.
        let threshold = ctx.config.l1_shard_split_threshold();
        let entries: Vec<(Bytes, WalPosition)> = build_table(1000).iter().collect();
        let mut shard_tables: Vec<(Vec<u8>, Vec<u8>, IndexTable)> = Vec::new();
        IndexFlusherThread::split_recursive(&entries, &ctx.ks_config, threshold, &mut shard_tables);
        for (_, _, t) in &shard_tables {
            assert!(serialized_size(&ctx.ks_config, t) <= threshold);
        }
        assert_eq!(shard_tables.len(), new_levels.shards().len());
    }

    #[test]
    fn split_recursive_singleton_when_one_entry_still_too_big() {
        // Defensive: with size_limit=0 every shard is "too big", but
        // split_recursive bottoms out at len=1 instead of recursing forever.
        let ctx = make_ctx(true, 1024 * 1024);
        let entries: Vec<(Bytes, WalPosition)> = build_table(4).iter().collect();
        let mut out: Vec<(Vec<u8>, Vec<u8>, IndexTable)> = Vec::new();
        IndexFlusherThread::split_recursive(&entries, &ctx.ks_config, 0, &mut out);
        assert_eq!(out.len(), 4, "len=1 shards must be accepted defensively");
        for (min_key, max_key, _) in &out {
            assert_eq!(min_key, max_key, "single-entry shard has min==max");
        }
    }
}
