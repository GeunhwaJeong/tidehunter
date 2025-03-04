pub mod index_table;
pub mod microcell;
pub mod persisted_index;
pub mod single_hop;

use single_hop::SingleHopIndex;
use std::sync::LazyLock;
pub static PERSISTED_INDEX: LazyLock<SingleHopIndex> = LazyLock::new(|| SingleHopIndex::new());

// use microcell::MicroCellIndex;
// pub static PERSISTED_INDEX: MicroCellIndex = MicroCellIndex;
