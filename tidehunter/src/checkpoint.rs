use crate::context::DbOpKind;
use crate::db::{Db, DbResult};
use crate::key_shape::KeySpace;
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
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::db::Db;
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
}
