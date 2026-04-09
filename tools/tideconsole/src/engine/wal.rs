use super::{DbHandle, Entry, from_wal_entry};
use rhai::{Dynamic, Engine, EvalAltResult, FnPtr, Map, NativeCallContext};
use std::path::Path;
use tidehunter::WalKind;
use tidehunter::config::Config;
use tidehunter::key_shape::KeySpace;
use tidehunter::test_utils::{
    IndexFormat, Metrics, Wal, WalEntry, WalError, list_wal_files_with_sizes,
};

// ---------------------------------------------------------------------------
// WAL walking helper (shared by walk_wal and wal_stats)
// ---------------------------------------------------------------------------

pub(crate) fn do_walk_wal<F>(
    db_path: &Path,
    config: &Config,
    start_pos: u64,
    skip_crc: bool,
    mut f: F,
) -> Result<(), Box<EvalAltResult>>
where
    F: FnMut(Entry) -> Result<(), Box<EvalAltResult>>,
{
    let metrics = Metrics::new();
    let wal = Wal::open(db_path, config.wal_layout(WalKind::Replay), metrics)
        .map_err(|e| -> Box<EvalAltResult> { format!("Failed to open WAL: {e:?}").into() })?;

    let mut iter = wal
        .wal_iterator(start_pos)
        .map_err(|e| -> Box<EvalAltResult> {
            format!("Failed to create WAL iterator: {e:?}").into()
        })?;
    iter.set_skip_crc(skip_crc);

    loop {
        match iter.next() {
            Ok((position, raw_bytes)) => {
                let raw_size = raw_bytes.len();
                let offset = position.offset();
                let entry = from_wal_entry(WalEntry::from_bytes(raw_bytes), offset, raw_size);
                f(entry)?;
            }
            // CRC errors and EndOfWal both signal the end of written data
            Err(WalError::Crc(_)) | Err(WalError::EndOfWal) => break,
            Err(WalError::Io(e)) => {
                return Err(format!("WAL I/O error: {e}").into());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(engine: &mut Engine) {
    // --- db.walk_wal(|entry| { ... }) ---
    engine.register_fn(
        "walk_wal",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            do_walk_wal(&db_path, &config, 0, false, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.walk_wal(start_pos, |entry| { ... }) ---
    engine.register_fn(
        "walk_wal",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         start_pos: i64,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            let start = u64::try_from(start_pos)
                .map_err(|_| -> Box<EvalAltResult> { "start_pos must be non-negative".into() })?;
            do_walk_wal(&db_path, &config, start, false, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.walk_wal_unchecked(|entry| { ... }) — skips CRC verification ---
    engine.register_fn(
        "walk_wal_unchecked",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            do_walk_wal(&db_path, &config, 0, true, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.walk_wal_unchecked(start_pos, |entry| { ... }) ---
    engine.register_fn(
        "walk_wal_unchecked",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         start_pos: i64,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };
            let start = u64::try_from(start_pos)
                .map_err(|_| -> Box<EvalAltResult> { "start_pos must be non-negative".into() })?;
            do_walk_wal(&db_path, &config, start, true, |entry| {
                let _: Dynamic = visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                Ok(())
            })?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.wal_stats() ---
    engine.register_fn(
        "wal_stats",
        |h: &mut DbHandle| -> Result<(), Box<EvalAltResult>> {
            let (db_path, config, key_shape, print) = {
                let state = h.0.lock();
                (
                    state.db_path.clone(),
                    state.config.clone(),
                    state.key_shape.clone(),
                    state.print_fn.clone(),
                )
            };

            let mut counts = [0usize; 5]; // record, remove, index, batch_start, drop_cells
            let mut total_bytes = 0usize;
            let mut ks_counts: std::collections::BTreeMap<u8, (usize, usize)> = Default::default(); // ks -> (records, removes)

            do_walk_wal(&db_path, &config, 0, false, |e| {
                total_bytes += e.raw_size as usize;
                match e.entry_type.as_str() {
                    "record" => {
                        counts[0] += 1;
                        ks_counts.entry(e.keyspace as u8).or_default().0 += 1;
                    }
                    "remove" => {
                        counts[1] += 1;
                        ks_counts.entry(e.keyspace as u8).or_default().1 += 1;
                    }
                    "index" => counts[2] += 1,
                    "batch_start" => counts[3] += 1,
                    "drop_cells" => counts[4] += 1,
                    _ => {}
                }
                Ok(())
            })?;

            let total = counts.iter().sum::<usize>();
            print("WAL Statistics");
            print("==============");
            print(&format!("  Total entries : {total}"));
            print(&format!("  Total bytes   : {total_bytes}"));
            print(&format!("  Records       : {}", counts[0]));
            print(&format!("  Removes       : {}", counts[1]));
            print(&format!("  Index         : {}", counts[2]));
            print(&format!("  BatchStart    : {}", counts[3]));
            print(&format!("  DropCells     : {}", counts[4]));

            if !ks_counts.is_empty() {
                print("\nPer-keyspace (records / removes):");
                for (ks_id, (recs, rems)) in &ks_counts {
                    let name = {
                        let space = KeySpace::new(*ks_id);
                        key_shape.ks(space).name().to_string()
                    };
                    print(&format!(
                        "  {name:35} records={recs:>10}  removes={rems:>10}"
                    ));
                }
            }
            Ok(())
        },
    );

    // --- db.list_wal_files() ---
    engine.register_fn(
        "list_wal_files",
        |h: &mut DbHandle| -> Result<rhai::Array, Box<EvalAltResult>> {
            let (db_path, config) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.config.clone())
            };

            let wal_file_size = config.wal_layout(WalKind::Replay).wal_file_size;

            let files = list_wal_files_with_sizes(&db_path).map_err(|e| -> Box<EvalAltResult> {
                format!("Failed to list WAL files: {e:?}").into()
            })?;

            let result = files
                .into_iter()
                .map(|(path, size)| {
                    let file_name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .to_string();

                    // File name: "wal_{id:016x}" — parse ID to derive start position.
                    let id = file_name
                        .strip_prefix("wal_")
                        .and_then(|s| u64::from_str_radix(s, 16).ok())
                        .unwrap_or(0);
                    let start_pos = (id * wal_file_size) as i64;

                    let created = std::fs::metadata(&path)
                        .ok()
                        .and_then(|m| m.created().ok().or_else(|| m.modified().ok()))
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(-1);

                    let mut map = rhai::Map::new();
                    map.insert("name".into(), Dynamic::from(file_name));
                    map.insert("start_pos".into(), Dynamic::from(start_pos));
                    map.insert("size".into(), Dynamic::from(size as i64));
                    map.insert("created".into(), Dynamic::from(created));
                    Dynamic::from(map)
                })
                .collect();

            Ok(result)
        },
    );

    // --- db.load_index(offset) ---
    //
    // Reads the index entry stored in the INDEX WAL at the given byte offset
    // (as returned by db.load_cr() cell.offset) and returns the key→data-WAL-position
    // mapping stored in that on-disk index.
    //
    // Returns an array of maps:
    //   [ { "key": "<hex>", "wal_position": <i64> }, ... ]
    //
    // Useful for verifying whether a specific write is reflected in the on-disk index
    // without doing a full WAL replay.
    engine.register_fn(
        "load_index",
        |h: &mut DbHandle, offset: i64| -> Result<rhai::Array, Box<EvalAltResult>> {
            let (db_path, config, key_shape) = {
                let state = h.0.lock();
                (
                    state.db_path.clone(),
                    state.config.clone(),
                    state.key_shape.clone(),
                )
            };

            if offset < 0 {
                return Err(format!("Invalid index offset {offset}: negative offsets indicate no index has been written for this cell").into());
            }
            let offset = offset as u64;

            let metrics = Metrics::new();
            let index_wal =
                Wal::open(&db_path, config.wal_layout(WalKind::Index), metrics).map_err(
                    |e| -> Box<EvalAltResult> {
                        format!("Failed to open index WAL: {e:?}").into()
                    },
                )?;

            let mut iter = index_wal
                .wal_iterator(offset)
                .map_err(|e| -> Box<EvalAltResult> {
                    format!("Failed to create index WAL iterator at offset {offset}: {e:?}").into()
                })?;

            let (position, raw_bytes) = iter.next().map_err(|e| -> Box<EvalAltResult> {
                format!("Failed to read index WAL entry at offset {offset}: {e:?}").into()
            })?;

            if position.offset() != offset {
                return Err(format!(
                    "Index WAL iterator landed at offset {} instead of requested {offset}; \
                     the offset may not be frame-aligned",
                    position.offset()
                )
                .into());
            }

            let entry = WalEntry::from_bytes(raw_bytes);
            let (ks_id, bytes) = match entry {
                WalEntry::Index(ks, bytes) => (ks, bytes),
                other => {
                    return Err(format!(
                        "Expected an Index entry at offset {offset} but found {:?}",
                        std::mem::discriminant(&other)
                    )
                    .into());
                }
            };

            let ks_desc = key_shape.ks(KeySpace::new(ks_id.as_u8()));
            let index_table = ks_desc.index_format().deserialize_index(ks_desc, bytes);

            let result: rhai::Array = index_table
                .iter()
                .map(|(key, wal_pos)| {
                    let mut m = Map::new();
                    m.insert("key".into(), Dynamic::from(hex::encode(&key)));
                    m.insert(
                        "wal_position".into(),
                        Dynamic::from(wal_pos.offset() as i64),
                    );
                    m.insert(
                        "payload_len".into(),
                        Dynamic::from(wal_pos.payload_len() as i64),
                    );
                    Dynamic::from(m)
                })
                .collect();

            Ok(result)
        },
    );
}
