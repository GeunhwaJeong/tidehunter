use std::collections::BTreeMap;
use std::ops::Range;

use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;

use crate::wal::WalPosition;
use crate::{
    index_table::IndexTable, key_shape::KeySpaceDesc, key_shape::CELL_PREFIX_LENGTH,
    lookup::RandomRead, math::rescale_u32,
};

const HEADER_ELEMENTS: usize = 1024;
const HEADER_ELEMENT_SIZE: usize = 8;
const HEADER_SIZE: usize = HEADER_ELEMENTS * HEADER_ELEMENT_SIZE;
const HALF_WINDOW_SIZE: usize = 500; // in entries (not bytes); todo make configurable
const PREFIX_LENGTH: usize = 8; // prefix of key used to estimate position in file, in bytes

pub trait PersistedIndex {
    fn to_bytes(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes;
    fn from_bytes(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable;
    fn lookup_unloaded(
        &self,
        ks: &KeySpaceDesc,
        reader: &impl RandomRead,
        key: &[u8],
    ) -> Option<WalPosition>;

    fn element_size(ks: &KeySpaceDesc) -> usize {
        ks.reduced_key_size() + WalPosition::LENGTH
    }

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

pub struct MicroCellIndex;

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

pub struct SingleHopIndex;

impl SingleHopIndex {
    fn get_probable_key_offset(
        cell_prefix_range: &Range<u64>,
        key: &[u8],
        file_length: usize,
    ) -> usize {
        let long_prefix_range_start = cell_prefix_range
            .start
            .saturating_mul(1 << (PREFIX_LENGTH - CELL_PREFIX_LENGTH) * 8);
        let long_prefix_range_end = cell_prefix_range
            .end
            .saturating_mul(1 << (PREFIX_LENGTH - CELL_PREFIX_LENGTH) * 8);
        let cell_width = long_prefix_range_end.saturating_sub(long_prefix_range_start);

        assert!(PREFIX_LENGTH >= CELL_PREFIX_LENGTH);
        assert!(PREFIX_LENGTH <= 8); // we want the prefix to fit in u64
        let mut p = [0u8; PREFIX_LENGTH];
        p[..PREFIX_LENGTH].copy_from_slice(&key[..PREFIX_LENGTH]);
        let prefix = u64::from_be_bytes(p);
        assert!(long_prefix_range_start <= prefix && prefix <= long_prefix_range_end);
        let prefix_pos = prefix.saturating_sub(long_prefix_range_start);
        let probable_offset = (prefix_pos as f64) * (file_length as f64) / (cell_width as f64);
        let probable_offset = (probable_offset.round() as usize).clamp(0, file_length - 1);
        probable_offset
    }

    fn get_start_and_end_offsets(
        estimated_offset: usize,
        element_size: usize,
        file_length: usize,
    ) -> (usize, usize) {
        // make sure to align to element size
        let estimated_offset = estimated_offset / element_size * element_size;
        let from_offset = estimated_offset.saturating_sub(HALF_WINDOW_SIZE * element_size);
        let to_offset = estimated_offset
            .saturating_add(HALF_WINDOW_SIZE * element_size)
            .clamp(0, file_length);
        (from_offset, to_offset)
    }

    fn move_window_up(
        from_offset: usize,
        to_offset: usize,
        element_size: usize,
        file_length: usize,
    ) -> (usize, usize) {
        let window_size = 2 * HALF_WINDOW_SIZE * element_size;
        let new_from_offset = to_offset;
        let new_to_offset = to_offset.saturating_add(window_size).clamp(0, file_length);
        (new_from_offset, new_to_offset)
    }

    fn move_window_down(
        from_offset: usize,
        to_offset: usize,
        element_size: usize,
        file_length: usize,
    ) -> (usize, usize) {
        let window_size = 2 * HALF_WINDOW_SIZE * element_size;
        let new_from_offset = from_offset
            .saturating_sub(window_size)
            .clamp(0, file_length);
        let new_to_offset = from_offset;
        (new_from_offset, new_to_offset)
    }
}

impl PersistedIndex for SingleHopIndex {
    fn to_bytes(&self, table: &IndexTable, ks: &KeySpaceDesc) -> Bytes {
        let element_size = Self::element_size(ks);
        let capacity = element_size * table.data.len();
        let mut out = BytesMut::with_capacity(capacity);
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
            out.put_slice(&key);
            value.write_to_buf(&mut out);
        }
        assert_eq!(out.len(), capacity);
        out.to_vec().into()
    }

    fn from_bytes(&self, ks: &KeySpaceDesc, b: Bytes) -> IndexTable {
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
        // compute cell and prefix
        let element_size = Self::element_size(ks);
        let key_size = ks.reduced_key_size();
        assert_eq!(key.len(), key_size);
        let prefix = ks.cell_prefix(key);
        let cell = ks.cell_by_prefix(prefix);
        let cell_prefix_range = ks.cell_prefix_range(cell);

        // compute probable offset
        let file_length = reader.len();
        let probable_offset = Self::get_probable_key_offset(&cell_prefix_range, key, file_length);

        // compute start and end of the window around probable offset
        let (mut from_offset, mut to_offset) =
            Self::get_start_and_end_offsets(probable_offset, element_size, file_length);

        loop {
            let buffer = reader.read(from_offset..to_offset);

            if buffer.len() < element_size {
                return None;
            }
            // first check if the buffer range contains the key
            let first_element_key = &buffer[..key_size];
            let last_element_key =
                &buffer[buffer.len() - element_size..buffer.len() - element_size + key_size];
            if key < first_element_key {
                // key is smaller than the first element in the buffer
                (from_offset, to_offset) =
                    Self::move_window_down(from_offset, to_offset, element_size, file_length);
                continue;
            }
            if key > last_element_key {
                // key is larger than the last element in the buffer
                (from_offset, to_offset) =
                    Self::move_window_up(from_offset, to_offset, element_size, file_length);
                continue;
            }

            // if the key is in the index, it must be in this buffer, iterate over the buffer to find the key
            // todo: binary search
            let mut buffer = &buffer[..];
            while !buffer.is_empty() {
                let k = &buffer[..key_size];
                if k == key {
                    buffer = &buffer[key_size..];
                    let position = WalPosition::read_from_buf(&mut buffer);
                    return Some(position);
                }
                buffer = &buffer[element_size..];
            }
            return None;
        }
    }
}

#[cfg(test)]
mod test {
    use minibytes::Bytes;
    use rand::{rngs::ThreadRng, Rng, RngCore};

    use crate::{
        index_table::IndexTable,
        key_shape::KeyShape,
        persisted_index::{MicroCellIndex, PersistedIndex, SingleHopIndex},
        wal::WalPosition,
    };

    #[test]
    fn test_offset_calculation() {
        let cell_prefix_range = 0..100;
        let file_length = 1000; // test with file larger than range

        let key = [0, 0, 0, 0, 0, 0, 0, 0];
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key: [u8; 9] = [0, 0, 0, 0, 0, 0, 0, 0, 255];
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64(0);
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64((cell_prefix_range.end << 32) - 1);
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length - 1);

        let key = k64((cell_prefix_range.end << 32) / 2);
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length / 2);

        let file_length = 10; // test with file smaller than range

        let key = [0, 0, 0, 0, 0, 0, 0, 0];
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key: [u8; 9] = [0, 0, 0, 0, 0, 0, 0, 0, 255];
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64(0);
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, 0);

        let key = k64((cell_prefix_range.end << 32) - 1);
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length - 1);

        let key = k64((cell_prefix_range.end << 32) / 2);
        let offset = SingleHopIndex::get_probable_key_offset(&cell_prefix_range, &key, file_length);
        assert_eq!(offset, file_length / 2);
    }

    #[test]
    pub fn test_index_lookup() {
        test_index_lookup_inner(&MicroCellIndex);
        test_index_lookup_inner(&SingleHopIndex);
    }

    pub fn test_index_lookup_inner(pi: &impl PersistedIndex) {
        let (shape, ks) = KeyShape::new_single(16, 8, 8);
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        index.insert(k128(1), w(5));
        index.insert(k128(5), w(10));

        let bytes = pi.to_bytes(&index, ks);
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(0)));
        assert_eq!(Some(w(5)), pi.lookup_unloaded(ks, &bytes, &k128(1)));
        assert_eq!(Some(w(10)), pi.lookup_unloaded(ks, &bytes, &k128(5)));
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(10)));
        let mut index = IndexTable::default();
        index.insert(k128(u128::MAX), w(15));
        index.insert(k128(u128::MAX - 5), w(25));
        let bytes = pi.to_bytes(&index, ks);
        assert_eq!(
            Some(w(15)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX))
        );
        assert_eq!(
            Some(w(25)),
            pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 5))
        );
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 1)));
        assert_eq!(None, pi.lookup_unloaded(ks, &bytes, &k128(u128::MAX - 100)));
    }

    #[test]
    pub fn test_index_lookup_random() {
        test_index_lookup_random_inner(&MicroCellIndex);
        test_index_lookup_random_inner(&SingleHopIndex);
    }

    pub fn test_index_lookup_random_inner(pi: &impl PersistedIndex) {
        const M: usize = 8;
        const P: usize = 8;
        let (shape, ks) = KeyShape::new_single(16, M, P);
        let ks = shape.ks(ks);
        let mut index = IndexTable::default();
        let mut rng = ThreadRng::default();
        let target_bucket = rng.gen_range(0..((M * P) as u128));
        let bucket_size = u128::MAX / ((M * P) as u128);
        let target_range = target_bucket * bucket_size..(target_bucket + 1) * bucket_size;
        const ITERATIONS: usize = 1000;
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            let pos = rng.next_u64();
            index.insert(k128(key), w(pos));
        }
        let bytes = pi.to_bytes(&index, ks);
        for (key, expected_value) in index.data {
            let value = pi.lookup_unloaded(ks, &bytes, &key);
            assert_eq!(Some(expected_value), value);
        }
        for _ in 0..ITERATIONS {
            let key = rng.gen_range(target_range.clone());
            let value = pi.lookup_unloaded(ks, &bytes, &k128(key));
            assert!(value.is_none());
        }
    }

    fn k64(k: u64) -> [u8; 8] {
        k.to_be_bytes()
    }

    fn k128(k: u128) -> Bytes {
        k.to_be_bytes().to_vec().into()
    }

    fn w(w: u64) -> WalPosition {
        WalPosition::test_value(w)
    }
}
