use super::DbHandle;
use rhai::{Dynamic, Engine, EvalAltResult, Map};
use tidehunter::CellId;
use tidehunter::control::ControlRegion;
use tidehunter::db::CONTROL_REGION_FILE;
use tidehunter::key_shape::KeySpace;

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(engine: &mut Engine) {
    // --- db.load_cr() → map ---
    //
    // Reads the control region file without opening the database (no WAL replay).
    // Returns a Rhai map with the full raw contents of the ControlRegion struct:
    //
    //   {
    //     "last_position": i64,        // WAL offset at which replay stopped
    //     "keyspaces": [               // one entry per keyspace, in keyspace-ID order
    //       {
    //         "name":        string,
    //         "id":          i64,
    //         "cells": [               // every cell in the snapshot (including empty ones)
    //           {
    //             "cell_id":        i64 | string,  // Integer → i64; Bytes → lowercase hex
    //             "offset":         i64,           // index WAL offset; -1 if no index yet
    //             "len":            i64,           // index frame length; 0 if no index yet
    //             "last_processed": i64,           // WAL offset up to which index is current
    //           },
    //           ...
    //         ]
    //       },
    //       ...
    //     ]
    //   }
    engine.register_fn(
        "load_cr",
        |h: &mut DbHandle| -> Result<Dynamic, Box<EvalAltResult>> {
            let (db_path, key_shape) = {
                let state = h.0.lock();
                (state.db_path.clone(), state.key_shape.clone())
            };

            let control_path = db_path.join(CONTROL_REGION_FILE);
            let cr = ControlRegion::read(&control_path, &key_shape).map_err(
                |e| -> Box<EvalAltResult> { format!("Failed to read control region: {e}").into() },
            )?;

            let snapshot = cr.snapshot();
            let mut ks_array: rhai::Array = Vec::new();

            for (ks_idx, ks_data) in snapshot.data.iter().enumerate() {
                let name = key_shape.ks(KeySpace::new(ks_idx as u8)).name().to_string();

                let cells: rhai::Array = ks_data
                    .iter()
                    .map(|(cell_id, entry)| {
                        let cell_id_val: Dynamic = match cell_id {
                            CellId::Integer(n) => Dynamic::from(*n as i64),
                            CellId::Bytes(b) => Dynamic::from(hex::encode(b.as_slice())),
                        };
                        let (offset, len) = if entry.position.is_valid() {
                            (
                                entry.position.offset() as i64,
                                entry.position.frame_len() as i64,
                            )
                        } else {
                            (-1i64, 0i64)
                        };

                        let mut cell_map = Map::new();
                        cell_map.insert("cell_id".into(), cell_id_val);
                        cell_map.insert("offset".into(), Dynamic::from(offset));
                        cell_map.insert("len".into(), Dynamic::from(len));
                        cell_map.insert(
                            "last_processed".into(),
                            Dynamic::from(entry.last_processed.as_u64() as i64),
                        );
                        Dynamic::from(cell_map)
                    })
                    .collect();

                let mut ks_map = Map::new();
                ks_map.insert("name".into(), Dynamic::from(name));
                ks_map.insert("id".into(), Dynamic::from(ks_idx as i64));
                ks_map.insert("cells".into(), Dynamic::from(cells));
                ks_array.push(Dynamic::from(ks_map));
            }

            let mut map = Map::new();
            map.insert(
                "last_position".into(),
                Dynamic::from(cr.last_position() as i64),
            );
            map.insert("keyspaces".into(), Dynamic::from(ks_array));
            Ok(Dynamic::from(map))
        },
    );
}
