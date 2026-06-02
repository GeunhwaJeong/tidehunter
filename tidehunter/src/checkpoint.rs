use crate::cell::CellId;
use crate::context::DbOpKind;
use crate::db::{Db, DbResult};
use crate::index::index_format::IndexIterCaches;
use crate::iterators::IteratorResult;
use crate::iterators::db_iterator::{DbIterator, IterationSource};
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::GetResult;
use crate::wal::tracker::WalTrackerLatch;
use minibytes::Bytes;
use std::sync::Arc;

/// A stable, point-in-time read view of a [`Db`], created by [`Db::checkpoint`].
///
/// Reads observe the database state as of the WAL frontier captured when the
/// checkpoint was created: values written before the checkpoint are visible,
/// values written afterward are not — even as the underlying database keeps
/// advancing. The view stays stable for the lifetime of this object; dropping
/// it releases the latch and lets the WAL tracker's external frontier catch up.
pub struct DbCheckpoint {
    db: Arc<Db>,
    latch: WalTrackerLatch,
}

impl DbCheckpoint {
    pub(crate) fn new(db: Arc<Db>, latch: WalTrackerLatch) -> Self {
        Self { db, latch }
    }

    /// Reads `k` from `ks` as of the checkpoint frontier.
    ///
    /// Unlike [`Db::get`], this bypasses the value LRU and bloom filter (both
    /// reflect the latest database state, not the checkpoint) and only
    /// considers index positions at or below the latched frontier.
    pub fn get(&self, ks: KeySpace, k: &[u8]) -> DbResult<Option<Bytes>> {
        let db = self.db.as_ref();
        let context = db.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Get);
        let reduced_key = context.ks_config.reduce_key(k);
        let last_processed = self.latch.position();
        match db
            .large_table
            .get_checkpoint(context, reduced_key.as_ref(), last_processed, db)?
        {
            // No LRU is consulted on this path, so `Value` is never produced;
            // handle it for completeness with the same key check as `Db::get`.
            GetResult::Value(full_key, value) => {
                if context.ks_config.need_check_index_key() && full_key.as_ref() != k {
                    return Ok(None);
                }
                Ok(Some(value))
            }
            GetResult::WalPosition(w) => db.read_record_check_key(context, k, w),
            GetResult::NotFound => Ok(None),
        }
    }

    /// Ordered iterator over the checkpoint's snapshot for `ks`.
    ///
    /// The returned [`DbIterator`] holds an `Arc` to this checkpoint, so the
    /// latch stays pinned and the view stays stable for the iterator's whole
    /// lifetime, independent of writes to the live database.
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        DbIterator::new(IterationSource::Checkpoint(self.clone()), ks)
    }

    pub(crate) fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        self.db.ks(ks)
    }

    pub(crate) fn next_cell(
        &self,
        ks: &KeySpaceDesc,
        cell: &CellId,
        reverse: bool,
    ) -> Option<CellId> {
        self.db.next_cell(ks, cell, reverse)
    }

    /// Checkpoint counterpart of [`Db::next_entry`]: walks the index as of the
    /// latched frontier (overlay positions at or above it are skipped) and
    /// reads matching records from the WAL without touching the value LRU.
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        cell: CellId,
        prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
    ) -> DbResult<Option<IteratorResult<Bytes>>> {
        let db = self.db.as_ref();
        let context = db.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::NextEntry);
        let last_processed = self.latch.position();
        let Some(result) = db.large_table.next_entry(
            context,
            cell,
            prev_key,
            db,
            end_cell_exclusive,
            reverse,
            cache,
            Some(last_processed),
        )?
        else {
            return Ok(None);
        };
        let (key, value) = match result.value {
            // No LRU is consulted on the checkpoint path, so `Value` is never
            // produced; handle it for completeness.
            GetResult::Value(ref full_key, ref v) => (full_key.clone(), v.clone()),
            GetResult::WalPosition(w) => {
                let Some((k, v)) =
                    db.read_record_for_indexed_key(context, w, result.key.as_ref())?
                else {
                    return Ok(None);
                };
                // Deliberately no `update_lru`: a checkpoint read must not
                // mutate the live database's value cache.
                (k, v)
            }
            GetResult::NotFound => unreachable!(),
        };
        Ok(Some(result.with_key_value(key, value)))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::db::{Db, DbResult};
    use crate::key_shape::{KeyShape, KeyType};
    use crate::metrics::Metrics;
    use minibytes::Bytes;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_checkpoint_snapshot_read() {
        let dir = tempdir::TempDir::new("test-checkpoint").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();

        let key = 42u64.to_be_bytes().to_vec();
        let v1: Bytes = vec![1u8].into();
        let v2: Bytes = vec![2u8].into();

        // Insert the original value, then snapshot the database at this point.
        db.insert(ks, key.clone(), v1.clone()).unwrap();
        let checkpoint = db.checkpoint();

        // Update the same key to a new value after the checkpoint was taken.
        db.insert(ks, key.clone(), v2.clone()).unwrap();

        // The checkpoint still observes the value as of when it was created...
        assert_eq!(
            Some(v1.clone()),
            checkpoint.get(ks, &key).unwrap(),
            "checkpoint must return the pre-checkpoint value"
        );
        // ...while a direct read sees the new value.
        assert_eq!(
            Some(v2.clone()),
            db.get(ks, &key).unwrap(),
            "direct read must return the latest value"
        );

        // The snapshot stays stable over time even as background promotion runs:
        // the latch pins the external `last_processed`, so the new value is never
        // promoted past the checkpoint frontier, and the offset filter keeps
        // hiding it.
        thread::sleep(Duration::from_millis(1));
        assert_eq!(
            Some(v1.clone()),
            checkpoint.get(ks, &key).unwrap(),
            "checkpoint must remain stable after waiting"
        );
        assert_eq!(Some(v2.clone()), db.get(ks, &key).unwrap());

        // A second checkpoint taken after the update observes the new value —
        // even while the first checkpoint is still held. Each checkpoint reads
        // as of its own captured frontier; the older latch only pins the
        // externally observed `last_processed`, it does not hold back a newer
        // checkpoint's view.
        let checkpoint2 = db.checkpoint();
        assert_eq!(
            Some(v2.clone()),
            checkpoint2.get(ks, &key).unwrap(),
            "a later checkpoint must observe the new value despite an older one being held"
        );

        // ...and the first checkpoint keeps returning its own snapshot value,
        // unaffected by the existence of the second checkpoint.
        assert_eq!(
            Some(v1.clone()),
            checkpoint.get(ks, &key).unwrap(),
            "the original checkpoint must remain stable alongside a newer one"
        );
    }

    #[test]
    fn test_checkpoint_iterator_snapshot() {
        let dir = tempdir::TempDir::new("test-checkpoint-iter").unwrap();
        let config = Arc::new(Config::small());
        let (key_shape, ks) = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();

        let k = |n: u64| n.to_be_bytes().to_vec();
        let v = |b: u8| -> Bytes { vec![b].into() };

        // Seed three keys, then snapshot.
        db.insert(ks, k(1), v(10)).unwrap();
        db.insert(ks, k(2), v(20)).unwrap();
        db.insert(ks, k(3), v(30)).unwrap();
        let checkpoint = db.checkpoint();

        // Mutate the live db after the checkpoint: update k1, delete k2, add k4.
        db.insert(ks, k(1), v(11)).unwrap();
        db.remove(ks, k(2)).unwrap();
        db.insert(ks, k(4), v(40)).unwrap();

        let collect = |it: crate::iterators::db_iterator::DbIterator| -> Vec<(Bytes, Bytes)> {
            it.collect::<DbResult<Vec<_>>>().unwrap()
        };

        // The checkpoint iterator yields the snapshot state, ignoring later
        // writes: k1 keeps its old value, the deleted k2 is still present, and
        // the newly inserted k4 is absent.
        let snapshot = collect(checkpoint.iterator(ks));
        assert_eq!(
            snapshot,
            vec![
                (k(1).into(), v(10)),
                (k(2).into(), v(20)),
                (k(3).into(), v(30)),
            ],
            "checkpoint iterator must reflect the snapshot, not later writes"
        );

        // A live iterator reflects the mutations.
        let live = collect(db.iterator(ks));
        assert_eq!(
            live,
            vec![
                (k(1).into(), v(11)),
                (k(3).into(), v(30)),
                (k(4).into(), v(40)),
            ],
            "live iterator must reflect the latest writes"
        );

        // Reverse iteration over the checkpoint yields the snapshot in
        // descending key order.
        let mut rev_iter = checkpoint.iterator(ks);
        rev_iter.reverse();
        assert_eq!(
            collect(rev_iter),
            vec![
                (k(3).into(), v(30)),
                (k(2).into(), v(20)),
                (k(1).into(), v(10)),
            ],
            "reverse checkpoint iterator must reflect the snapshot in reverse"
        );

        // Stable over time even as background promotion runs.
        thread::sleep(Duration::from_millis(1));
        assert_eq!(
            snapshot,
            collect(checkpoint.iterator(ks)),
            "checkpoint iterator must stay stable after waiting"
        );
    }
}
