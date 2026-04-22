use crate::WalPosition;
use crate::key_shape::KeyShape;
use crate::large_table::{LargeTableContainer, SnapshotEntryData};
use crate::metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};

/// On-disk control-region format version.
///
/// The file layout is either:
///
/// - **V1** (legacy): raw `bincode`-serialized [`legacy_v1::ControlRegionV1`]
///   starting at byte 0. No header. Distinguished by the fact that the first
///   8 bytes decode as a plausible `last_position: u64` — never the V2
///   sentinel.
/// - **V2**: 8-byte little-endian sentinel `u64::MAX`, followed by a 4-byte
///   little-endian version number, followed by `bincode`-serialized
///   [`ControlRegion`]. Using `u64::MAX` as the discriminator works because
///   the first field of the V1 layout is `last_position: u64`, which would
///   only take the value `u64::MAX` in a pathological/corrupt file — never
///   in a real database.
///
/// Writers always emit V2; readers detect the sentinel and fall back to V1
/// parsing + one-way migration.
const CONTROL_REGION_V2_SENTINEL: u64 = u64::MAX;
const CONTROL_REGION_V2_VERSION: u32 = 2;
const CONTROL_REGION_V2_HEADER_LEN: usize = 12;

#[derive(Serialize, Deserialize, Debug)]
#[doc(hidden)] // Used by tools/tideconsole for control region inspection
pub struct ControlRegion {
    /// 0 when wal is empty or nothing has been processed
    last_position: u64,
    snapshot: LargeTableContainer<SnapshotEntryData>,
    /// Keyspace names in order - used to verify that keyspaces haven't been reordered
    /// or inserted in the middle. Only new keyspaces can be added at the end.
    #[serde(default)]
    keyspace_names: Vec<String>,
}

pub(crate) struct ControlRegionStore {
    path: PathBuf,
    last_position: u64,
    force_relocation_position: Option<WalPosition>,
}

impl ControlRegion {
    pub fn new_empty(key_shape: &KeyShape) -> Self {
        let snapshot =
            LargeTableContainer::new_from_key_shape(key_shape, SnapshotEntryData::empty());
        let keyspace_names = key_shape
            .iter_ks()
            .map(|ks| ks.name().to_string())
            .collect();
        Self {
            last_position: 0,
            snapshot,
            keyspace_names,
        }
    }

    pub fn new(
        snapshot: LargeTableContainer<SnapshotEntryData>,
        last_position: u64,
        key_shape: &KeyShape,
    ) -> Self {
        let keyspace_names = key_shape
            .iter_ks()
            .map(|ks| ks.name().to_string())
            .collect();
        Self {
            snapshot,
            last_position,
            keyspace_names,
        }
    }

    pub fn read_or_create(path: &Path, key_shape: &KeyShape) -> Self {
        match Self::read(path, key_shape) {
            Ok(control_region) => control_region,
            Err(err) if err.kind() == ErrorKind::NotFound => ControlRegion::new_empty(key_shape),
            Err(err) => {
                panic!("Failed to read control region file: {err:?}")
            }
        }
    }

    #[doc(hidden)] // Used by tools/tideconsole for control region inspection
    pub fn read(path: &Path, key_shape: &KeyShape) -> io::Result<Self> {
        let bytes = fs::read(path)?;
        let mut control_region = Self::deserialize_any_version(&bytes)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        control_region.populate_keyspace_names_if_empty(key_shape);
        control_region.verify_shape(key_shape);
        control_region.extend_snapshot_if_needed(key_shape);
        Ok(control_region)
    }

    /// Deserializes a control region from bytes, transparently handling
    /// both V1 (pre-levels) and V2 (with `IndexLevels`) formats.
    ///
    /// V1 control regions are migrated on read: each single-position cell
    /// becomes a one-level `IndexLevels`. The migration is one-way — the
    /// next successful `store` rewrites the file in V2 format.
    fn deserialize_any_version(bytes: &[u8]) -> Result<Self, bincode::Error> {
        if bytes.len() >= CONTROL_REGION_V2_HEADER_LEN {
            let sentinel = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            if sentinel == CONTROL_REGION_V2_SENTINEL {
                let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
                assert_eq!(
                    version, CONTROL_REGION_V2_VERSION,
                    "Unsupported control region version {version}; expected \
                     {CONTROL_REGION_V2_VERSION}. This binary is older than the \
                     database it is trying to open."
                );
                let payload = &bytes[CONTROL_REGION_V2_HEADER_LEN..];
                return bincode::deserialize::<ControlRegion>(payload);
            }
        }
        // No sentinel ⇒ legacy V1 layout.
        let v1: legacy_v1::ControlRegionV1 = bincode::deserialize(bytes)?;
        Ok(v1.migrate())
    }

    /// Populates keyspace names from key_shape if they're empty (for old control regions)
    fn populate_keyspace_names_if_empty(&mut self, key_shape: &KeyShape) {
        if self.keyspace_names.is_empty() {
            // This is an old control region without keyspace names
            // Populate names for existing keyspaces from the current key_shape
            self.keyspace_names = key_shape
                .iter_ks()
                .take(self.snapshot.data.len())
                .map(|ks| ks.name().to_string())
                .collect();
        }
    }

    /// Extends the snapshot to include new keyspaces if the key_shape has more keyspaces
    fn extend_snapshot_if_needed(&mut self, key_shape: &KeyShape) {
        let snapshot_len = self.snapshot.data.len();
        let num_ks = key_shape.num_ks();

        if snapshot_len < num_ks {
            // Add empty entries for new keyspaces
            for ks in key_shape.iter_ks().skip(snapshot_len) {
                let new_ks_snapshot =
                    LargeTableContainer::new_keyspace(ks, SnapshotEntryData::empty());
                self.snapshot.data.push(new_ks_snapshot);
                self.keyspace_names.push(ks.name().to_string());
            }
        }
    }

    fn verify_shape(&self, key_shape: &KeyShape) {
        let snapshot_len = self.snapshot.data.len();
        let num_ks = key_shape.num_ks();

        // We allow adding keyspaces at the end, but not removing or reordering
        if snapshot_len > num_ks {
            panic!(
                "Control region has {snapshot_len} key spaces, while configuration has {num_ks}. Removing key spaces is not supported"
            );
        }

        // Verify that existing keyspaces match by name in the same order
        // This ensures that keyspaces are only added at the end, not inserted in the middle
        for (idx, ks_desc) in key_shape.iter_ks().enumerate().take(snapshot_len) {
            let current_name = ks_desc.name();

            // Check if we have stored keyspace names (backward compatibility: might be empty for old control regions)
            if idx < self.keyspace_names.len() {
                let stored_name = &self.keyspace_names[idx];
                if current_name != stored_name {
                    panic!(
                        "Keyspace mismatch at position {idx}: control region has keyspace '{stored_name}', \
                         but configuration has '{current_name}'. \
                         Reordering or renaming keyspaces is not supported. \
                         Only adding new keyspaces at the end is allowed."
                    );
                }
            }
        }
    }

    pub fn snapshot(&self) -> &LargeTableContainer<SnapshotEntryData> {
        &self.snapshot
    }

    pub fn last_position(&self) -> u64 {
        self.last_position
    }

    pub fn last_index_wal_position(&self) -> Option<WalPosition> {
        self.snapshot.iter_valid_val_positions().max()
    }
}

const FORCE_RELOCATION_PCT: usize = 99;

impl ControlRegionStore {
    pub fn new(path: PathBuf, control_region: &ControlRegion) -> Self {
        Self {
            path,
            last_position: control_region.last_position,
            force_relocation_position: control_region
                .snapshot
                .pct_wal_position(FORCE_RELOCATION_PCT),
        }
    }

    pub fn store(
        &mut self,
        snapshot: LargeTableContainer<SnapshotEntryData>,
        last_position: u64,
        key_shape: &KeyShape,
        metrics: &Metrics,
    ) {
        assert!(
            last_position >= self.last_position,
            "control region last_position regressed: new={last_position} < old={}",
            self.last_position,
        );
        let force_relocation_position = snapshot.pct_wal_position(FORCE_RELOCATION_PCT);
        let control_region = ControlRegion::new(snapshot, last_position, key_shape);
        // V2 layout: sentinel + version + bincode payload. See the comment
        // on `CONTROL_REGION_V2_SENTINEL` for the rationale.
        let mut serialized = Vec::with_capacity(CONTROL_REGION_V2_HEADER_LEN + 4 * 1024);
        serialized.extend_from_slice(&CONTROL_REGION_V2_SENTINEL.to_le_bytes());
        serialized.extend_from_slice(&CONTROL_REGION_V2_VERSION.to_le_bytes());
        bincode::serialize_into(&mut serialized, &control_region)
            .expect("Failed to serialize control region");
        let temp_file = self.path.with_extension(".bak");
        fs::write(&temp_file, &serialized).unwrap_or_else(|e| {
            panic!(
                "Failed to write control region file {}: {}",
                temp_file.display(),
                e
            )
        });
        metrics
            .snapshot_written_bytes
            .inc_by(serialized.len() as u64);
        fs::rename(&temp_file, &self.path).unwrap_or_else(|e| {
            panic!(
                "Failed to rename control region file from {} to {}: {}",
                temp_file.display(),
                self.path.display(),
                e
            )
        });
        self.last_position = last_position;
        self.force_relocation_position = force_relocation_position;
    }

    /// The path to the control region file
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn last_position(&self) -> u64 {
        self.last_position
    }

    pub fn force_relocation_position(&self) -> Option<WalPosition> {
        self.force_relocation_position
    }
}

/// Legacy on-disk representation — read-only, used for one-way migration of
/// V1 control regions.
///
/// V1 stored a single `WalPosition` per cell. V2 stores an `IndexLevels`
/// (`SmallVec<[WalPosition; 2]>`). The migration maps every valid single
/// position to a one-element level list; `WalPosition::INVALID` maps to an
/// empty list.
mod legacy_v1 {
    use super::ControlRegion;
    use crate::cell::CellId;
    use crate::index::levels::IndexLevels;
    use crate::large_table::{LargeTableContainer, SnapshotEntryData};
    use crate::wal::position::{LastProcessed, WalPosition};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    /// Exact on-disk layout of `SnapshotEntryData` prior to the two-level
    /// LSM schema change. Field names and ordering must stay in sync with
    /// the pre-change definition.
    #[derive(Serialize, Deserialize, Clone, Copy, Debug)]
    pub(super) struct SnapshotEntryDataV1 {
        pub(super) position: WalPosition,
        pub(super) last_processed: LastProcessed,
    }

    impl SnapshotEntryDataV1 {
        fn migrate(self) -> SnapshotEntryData {
            SnapshotEntryData::from_levels(
                IndexLevels::from_legacy_position(self.position),
                self.last_processed,
            )
        }
    }

    /// Exact on-disk layout of `ControlRegion` prior to the two-level LSM
    /// schema change. `keyspace_names` keeps `#[serde(default)]` for
    /// pre-keyspace-names compatibility within V1 files.
    #[derive(Serialize, Deserialize, Debug)]
    pub(super) struct ControlRegionV1 {
        pub(super) last_position: u64,
        pub(super) snapshot: LargeTableContainer<SnapshotEntryDataV1>,
        #[serde(default)]
        pub(super) keyspace_names: Vec<String>,
    }

    impl ControlRegionV1 {
        pub(super) fn migrate(self) -> ControlRegion {
            let data: Vec<BTreeMap<CellId, SnapshotEntryData>> = self
                .snapshot
                .data
                .into_iter()
                .map(|ks_map| {
                    ks_map
                        .into_iter()
                        .map(|(cell, entry)| (cell, entry.migrate()))
                        .collect()
                })
                .collect();
            ControlRegion {
                last_position: self.last_position,
                snapshot: LargeTableContainer { data },
                keyspace_names: self.keyspace_names,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::CellId;
    use crate::index::levels::IndexLevels;
    use crate::key_shape::{KeyShape, KeyType, UniformKeyConfig};
    use crate::wal::position::{LastProcessed, WalPosition};
    use std::collections::BTreeMap;

    /// Returns a minimal key_shape with one uniform keyspace of a few cells,
    /// suitable for round-tripping control regions through disk.
    fn tiny_shape() -> KeyShape {
        let (shape, _ks) = KeyShape::new_single(
            /*key_size=*/ 8,
            /*mutexes=*/ 2,
            KeyType::Uniform(UniformKeyConfig::new(/*cells_per_mutex=*/ 2)),
        );
        shape
    }

    #[test]
    fn v2_roundtrip() {
        let dir = tempdir::TempDir::new("cr_v2_roundtrip").unwrap();
        let path = dir.path().join("cr");
        let shape = tiny_shape();

        let mut cr = ControlRegion::new_empty(&shape);
        // Populate one cell with a known level set so we can check we read
        // back the same bytes.
        cr.snapshot.data[0].insert(
            CellId::Integer(0),
            SnapshotEntryData::from_levels(
                IndexLevels::single(WalPosition::test_value(7777)),
                LastProcessed::new_test(42),
            ),
        );
        cr.last_position = 100;

        let mut store = ControlRegionStore::new(path.clone(), &cr);
        store.store(cr.snapshot, cr.last_position, &shape, &Metrics::new());

        let read_cr = ControlRegion::read(&path, &shape).unwrap();
        assert_eq!(read_cr.last_position, 100);
        let entry = read_cr.snapshot.data[0]
            .get(&CellId::Integer(0))
            .expect("cell 0 exists");
        assert_eq!(entry.levels.len(), 1);
        assert_eq!(entry.levels.latest(), Some(WalPosition::test_value(7777)));
        assert_eq!(entry.last_processed, LastProcessed::new_test(42));
    }

    #[test]
    fn v1_file_migrates_to_v2() {
        // Hand-build a V1 byte stream and read it through the public `read`
        // path to ensure the sentinel detection and migration do the right
        // thing on a file that predates the schema change.
        let dir = tempdir::TempDir::new("cr_v1_migrate").unwrap();
        let path = dir.path().join("cr_v1");
        let shape = tiny_shape();
        let num_ks = shape.num_ks();

        let mut ks_map: BTreeMap<CellId, legacy_v1::SnapshotEntryDataV1> = BTreeMap::new();
        ks_map.insert(
            CellId::Integer(0),
            legacy_v1::SnapshotEntryDataV1 {
                position: WalPosition::test_value(1234),
                last_processed: LastProcessed::new_test(10),
            },
        );
        ks_map.insert(
            CellId::Integer(1),
            legacy_v1::SnapshotEntryDataV1 {
                position: WalPosition::INVALID,
                last_processed: LastProcessed::none(),
            },
        );
        let mut data = vec![ks_map];
        // Pad to match shape's keyspace count; verify_shape requires equality.
        for _ in 1..num_ks {
            data.push(BTreeMap::new());
        }
        let keyspace_names: Vec<String> = shape.iter_ks().map(|ks| ks.name().to_string()).collect();
        let v1 = legacy_v1::ControlRegionV1 {
            last_position: 55,
            snapshot: LargeTableContainer { data },
            keyspace_names,
        };
        let bytes = bincode::serialize(&v1).unwrap();
        std::fs::write(&path, &bytes).unwrap();

        let read_cr = ControlRegion::read(&path, &shape).unwrap();
        assert_eq!(read_cr.last_position, 55);

        let cell0 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(0))
            .expect("cell 0 present");
        assert_eq!(cell0.levels.len(), 1);
        assert_eq!(cell0.levels.latest(), Some(WalPosition::test_value(1234)));

        let cell1 = read_cr.snapshot.data[0]
            .get(&CellId::Integer(1))
            .expect("cell 1 present");
        assert!(cell1.levels.is_empty(), "INVALID ⇒ empty level list");
    }
}
