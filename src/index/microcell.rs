use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use std::collections::BTreeMap;

use crate::math::rescale_u32;
use crate::wal::WalPosition;
use crate::{index::index_table::IndexTable, key_shape::KeySpaceDesc, lookup::RandomRead};

use super::persisted_index::{PersistedIndex, HEADER_ELEMENTS, HEADER_ELEMENT_SIZE, HEADER_SIZE};

pub struct MicroCellIndex;

impl MicroCellIndex {
    fn key_micro_cell(ks: &KeySpaceDesc, key: &[u8]) -> usize {
        let prefix = ks.cell_prefix(key);
        let cell = ks.cell_by_prefix(prefix);
        let cell_prefix_range = ks.cell_prefix_range(cell);
        let cell_offset = prefix
            // cell_prefix_range.start is always u32 (but not cell_prefix_range.end)
            .checked_sub(cell_prefix_range.start as u32)
            .expect("Key prefix is out of cell prefix range");
        let cell_size = ks.cell_size();
        let micro_cell = rescale_u32(cell_offset, cell_size, HEADER_ELEMENTS as u32);
        micro_cell as usize
    }
}

impl PersistedIndex for MicroCellIndex {
    fn to_bytes(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes {
        let element_size = Self::element_size(ks);
        let capacity = element_size * table.data.len() + HEADER_SIZE;
        let mut out = BytesMut::with_capacity(capacity);
        out.put_bytes(0, HEADER_SIZE);
        let mut header = IndexTableHeaderBuilder::new(ks);
        for (key, value) in table.data.iter() {
            if key.len() != ks.reduced_key_size() {
                // todo make into debug assertion
                panic!(
                    "Index in ks {} contains key length {} (configured {})",
                    ks.name(),
                    key.len(),
                    ks.reduced_key_size()
                );
            }
            header.add_key(key, out.len());
            out.put_slice(&key);
            value.write_to_buf(&mut out);
        }
        assert_eq!(out.len(), capacity);
        header.write_header(out.len(), &mut out[..HEADER_SIZE]);
        out.to_vec().into()
    }

    fn from_bytes(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable {
        let b = b.slice(HEADER_SIZE..);
        let element_size = Self::element_size(ks);
        let elements = b.len() / element_size;
        assert_eq!(b.len(), elements * element_size);

        let mut data = BTreeMap::new();
        for i in 0..elements {
            let key = b.slice(i * element_size..(i * element_size + ks.reduced_key_size()));
            let value = WalPosition::from_slice(
                &b[(i * element_size + ks.reduced_key_size())..(i * element_size + element_size)],
            );
            data.insert(key, value);
        }

        assert_eq!(data.len(), elements);
        IndexTable { data }
    }

    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
    ) -> Option<WalPosition> {
        let key_size = ks.reduced_key_size();
        assert_eq!(key.len(), key_size);
        let micro_cell = Self::key_micro_cell(ks, key);
        let header_element =
            reader.read(micro_cell * HEADER_ELEMENT_SIZE..(micro_cell + 1) * HEADER_ELEMENT_SIZE);
        let mut header_element = &header_element[..];
        let from_offset = header_element.get_u32() as usize;
        let to_offset = header_element.get_u32() as usize;
        if from_offset == 0 && to_offset == 0 {
            return None;
        }
        let buffer = reader.read(from_offset..to_offset);
        let mut buffer = &buffer[..];
        let element_size = Self::element_size(ks);
        while !buffer.is_empty() {
            let k = &buffer[..key_size];
            if k == key {
                buffer = &buffer[key_size..];
                let position = WalPosition::read_from_buf(&mut buffer);
                return Some(position);
            }
            buffer = &buffer[element_size..];
        }
        None
    }
}
pub struct IndexTableHeaderBuilder<'a> {
    ks: &'a KeySpaceDesc,
    header: Vec<(u32, u32)>,
    last_micro_cell: Option<usize>,
}

impl<'a> IndexTableHeaderBuilder<'a> {
    pub fn new(ks: &'a KeySpaceDesc) -> Self {
        let header = (0..HEADER_ELEMENTS).map(|_| (0, 0)).collect();
        Self {
            ks,
            header,
            last_micro_cell: None,
        }
    }

    pub fn add_key(&mut self, key: &[u8], offset: usize) {
        let offset = Self::check_offset(offset);
        let micro_cell = MicroCellIndex::key_micro_cell(&self.ks, key);
        if let Some(last_micro_cell) = self.last_micro_cell {
            if last_micro_cell == micro_cell {
                return;
            }
            self.header[last_micro_cell].1 = offset;
        }
        self.last_micro_cell = Some(micro_cell);
        self.header[micro_cell].0 = offset;
    }

    pub fn write_header(self, end_offset: usize, mut buf: &mut [u8]) {
        let mut header = self.header;
        let end_offset = Self::check_offset(end_offset);
        if let Some(last_micro_cell) = self.last_micro_cell {
            header[last_micro_cell].1 = end_offset;
        }
        for (start, end) in header {
            buf.put_u32(start);
            buf.put_u32(end);
        }
    }

    fn check_offset(offset: usize) -> u32 {
        assert!(offset < u32::MAX as usize, "Index table is too large");
        offset as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::persisted_index::test::*;

    #[test]
    pub fn test_index_lookup() {
        test_index_lookup_inner(&MicroCellIndex);
    }

    #[test]
    pub fn test_index_lookup_random() {
        test_index_lookup_random_inner(&MicroCellIndex);
    }
}
