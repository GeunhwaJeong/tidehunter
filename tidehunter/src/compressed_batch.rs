//! Compressed write-batch payload.
//!
//! A single `WriteBatch::commit()` writes one `WalEntry::CompressedBatch` to the
//! WAL. All large-table entries in the batch share the same `WalPosition` (the
//! position of the compressed entry). On point lookups the reader knows the key
//! it wants, decompresses the batch, and linear-scans for a matching record.
//! Tombstones inside a batch are visited during WAL replay only — they are
//! never targets of point reads.
//!
//! Inner uncompressed layout:
//! ```text
//! u32 count
//! per entry:
//!   u32 entry_len
//!   bytes (WalEntry-encoded; see WalEntry::write_into_bytes)
//! ```
//! Inner entries reuse the existing `WalEntry` wire format; only the
//! `Record(_, _, _, false)` and `Remove` variants ever appear here because
//! that's all `WriteBatch` produces.
use crate::crc::IntoBytesFixed;
use crate::db::WalEntry;
use crate::key_shape::KeySpace;
use bytes::{Buf, BufMut, BytesMut};
use minibytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum BatchCodec {
    Lz4 = 1,
}

impl BatchCodec {
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Lz4,
            other => panic!("Unknown compressed-batch codec byte {other} in WAL"),
        }
    }
}

/// On-the-wire body of a `WalEntry::CompressedBatch`. Owns the already-compressed
/// payload (so the surrounding WAL frame can be sized before being placed).
#[doc(hidden)] // Used by tools/tideconsole for WAL compression estimation
pub struct CompressedBatch {
    pub codec: BatchCodec,
    pub uncompressed_len: u32,
    pub body: Bytes,
}

/// Helper for the read paths: if `entry` is a `WalEntry::CompressedBatch`,
/// reassemble its body view and decompress. Returns `None` otherwise — caller
/// can fall through to the per-record `WalEntry::Record(..)` arm.
pub(crate) fn decompress_wal_entry(entry: WalEntry) -> Option<Bytes> {
    if let WalEntry::CompressedBatch(codec, uncompressed_len, body) = entry {
        Some(
            CompressedBatch {
                codec,
                uncompressed_len,
                body,
            }
            .decompress(),
        )
    } else {
        None
    }
}

impl CompressedBatch {
    #[doc(hidden)] // Used by tools/tideconsole for WAL compression estimation
    pub fn encode(entries: &[WalEntry], codec: BatchCodec) -> Self {
        let mut buf = BytesMut::new();
        buf.put_u32(entries.len() as u32);
        for e in entries {
            let entry_len = e.len();
            buf.put_u32(entry_len as u32);
            e.write_into_bytes(&mut buf);
        }
        let uncompressed_len = buf.len() as u32;
        let raw_body: bytes::Bytes = buf.into();
        let body: Bytes = match codec {
            BatchCodec::Lz4 => bytes::Bytes::from(lz4_flex::compress(&raw_body)).into(),
        };
        Self {
            codec,
            uncompressed_len,
            body,
        }
    }

    /// Decompress the body into an owned buffer suitable for repeated scanning.
    pub(crate) fn decompress(&self) -> Bytes {
        match self.codec {
            BatchCodec::Lz4 => {
                let out = lz4_flex::decompress(&self.body, self.uncompressed_len as usize)
                    .expect("CompressedBatch decompression failed");
                bytes::Bytes::from(out).into()
            }
        }
    }
}

/// Iterator over the inner `WalEntry`s of a decompressed batch body. Each
/// yielded entry's payload is a zero-copy slice into the buffer.
pub(crate) struct InnerIter {
    data: Bytes,
    offset: usize,
    remaining: u32,
}

impl InnerIter {
    pub(crate) fn new(decompressed: Bytes) -> Self {
        let count = (&decompressed[..]).get_u32();
        Self {
            data: decompressed,
            offset: 4,
            remaining: count,
        }
    }
}

impl Iterator for InnerIter {
    type Item = WalEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let mut hdr = &self.data[self.offset..];
        let entry_len = hdr.get_u32() as usize;
        let entry_start = self.offset + 4;
        let entry_end = entry_start + entry_len;
        let entry_bytes = self.data.slice(entry_start..entry_end);
        self.offset = entry_end;
        Some(WalEntry::from_bytes(entry_bytes))
    }
}

/// Linear-scan the decompressed body for a record matching `(target_ks, target_key)`.
///
/// Returns the value bytes (a zero-copy slice into `decompressed`) if found.
/// Tombstones for `target_key` are ignored — point lookups never see them
/// because the index removes the entry on a tombstone batch commit. Batch
/// dedup at write time guarantees at most one matching record per
/// `(target_ks, target_key)`.
pub(crate) fn find_record(
    decompressed: &Bytes,
    target_ks: KeySpace,
    target_key: &[u8],
) -> Option<Bytes> {
    find_record_by(decompressed, target_ks, |k| k == target_key).map(|(_, v)| v)
}

/// Linear-scan with a custom key matcher. Used by the iterator and relocation
/// paths, which only have the *indexed* (possibly reduced) key — they supply a
/// closure that re-derives the indexed key from each candidate's full key and
/// compares it to the target. Batch dedup at write time guarantees at most one
/// matching record per indexed key.
pub(crate) fn find_record_by<F>(
    decompressed: &Bytes,
    target_ks: KeySpace,
    mut matches: F,
) -> Option<(Bytes, Bytes)>
where
    F: FnMut(&[u8]) -> bool,
{
    for entry in InnerIter::new(decompressed.clone()) {
        if let WalEntry::Record(ks, key, value, _) = entry
            && ks == target_ks
            && matches(key.as_ref())
        {
            return Some((key, value));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(ks: u8, k: &[u8], v: &[u8]) -> WalEntry {
        WalEntry::Record(
            KeySpace(ks),
            Bytes::from(k.to_vec()),
            Bytes::from(v.to_vec()),
            false,
        )
    }

    fn remove(ks: u8, k: &[u8]) -> WalEntry {
        WalEntry::Remove(KeySpace(ks), Bytes::from(k.to_vec()))
    }

    #[test]
    fn round_trip_lz4() {
        let entries = vec![
            record(1, b"key-1", &[42u8; 256]),
            remove(2, b"key-2"),
            record(1, b"key-3", b"hello world"),
        ];
        let encoded = CompressedBatch::encode(&entries, BatchCodec::Lz4);
        assert_eq!(encoded.codec, BatchCodec::Lz4);
        let decoded = encoded.decompress();
        assert_eq!(decoded.len() as u32, encoded.uncompressed_len);

        let got: Vec<_> = InnerIter::new(decoded.clone()).collect();
        assert_eq!(got.len(), 3);

        let v = find_record(&decoded, KeySpace(1), b"key-1").expect("present");
        assert_eq!(&v[..], &[42u8; 256]);

        assert!(find_record(&decoded, KeySpace(2), b"key-2").is_none()); // tombstone
        assert!(find_record(&decoded, KeySpace(1), b"missing").is_none());
    }

    #[test]
    fn iter_preserves_insertion_order() {
        let entries = vec![
            record(0, b"a", b"v-a"),
            remove(0, b"b"),
            record(0, b"c", b"v-c"),
            remove(0, b"d"),
        ];
        let decoded = CompressedBatch::encode(&entries, BatchCodec::Lz4).decompress();
        let kinds: Vec<_> = InnerIter::new(decoded)
            .map(|e| match e {
                WalEntry::Record(_, k, _, _) => ("r", k.to_vec()),
                WalEntry::Remove(_, k) => ("t", k.to_vec()),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("r", b"a".to_vec()),
                ("t", b"b".to_vec()),
                ("r", b"c".to_vec()),
                ("t", b"d".to_vec()),
            ]
        );
    }

    #[test]
    fn find_record_disambiguates_keyspace() {
        // Same key bytes appear in two different keyspaces with distinct values.
        let entries = vec![
            record(1, b"shared", b"value-in-ks-1"),
            record(2, b"shared", b"value-in-ks-2"),
        ];
        let decoded = CompressedBatch::encode(&entries, BatchCodec::Lz4).decompress();
        assert_eq!(
            &find_record(&decoded, KeySpace(1), b"shared").unwrap()[..],
            b"value-in-ks-1"
        );
        assert_eq!(
            &find_record(&decoded, KeySpace(2), b"shared").unwrap()[..],
            b"value-in-ks-2"
        );
        assert!(find_record(&decoded, KeySpace(3), b"shared").is_none());
    }

    #[test]
    fn find_record_by_uses_predicate_and_keyspace() {
        let entries = vec![
            record(1, b"alpha", b"1a"),
            record(1, b"beta", b"1b"),
            record(2, b"alpha", b"2a"),
            remove(1, b"alpha"), // tombstone in ks=1 must be ignored by find_record_by
        ];
        let decoded = CompressedBatch::encode(&entries, BatchCodec::Lz4).decompress();

        // Match the first record whose key starts with 'b' in ks=1.
        let (k, v) = find_record_by(&decoded, KeySpace(1), |k| k.starts_with(b"b")).unwrap();
        assert_eq!(&k[..], b"beta");
        assert_eq!(&v[..], b"1b");

        // Wrong keyspace — no match.
        assert!(find_record_by(&decoded, KeySpace(2), |k| k.starts_with(b"b")).is_none());

        // Predicate that never matches.
        assert!(find_record_by(&decoded, KeySpace(1), |_| false).is_none());
    }

    #[test]
    fn empty_batch_round_trips() {
        let encoded = CompressedBatch::encode(&[], BatchCodec::Lz4);
        let decoded = encoded.decompress();
        // count(4 bytes) + zero entries
        assert_eq!(encoded.uncompressed_len, 4);
        assert_eq!(InnerIter::new(decoded.clone()).count(), 0);
        assert!(find_record(&decoded, KeySpace(0), b"anything").is_none());
    }

    #[test]
    fn large_value_round_trips() {
        // Exercise the u32 value_len path with a value bigger than u16::MAX.
        let big = vec![0x5au8; 200_000];
        let entries = vec![record(7, b"big", &big)];
        let decoded = CompressedBatch::encode(&entries, BatchCodec::Lz4).decompress();
        let v = find_record(&decoded, KeySpace(7), b"big").unwrap();
        assert_eq!(v.len(), big.len());
        assert_eq!(&v[..], &big[..]);
    }

    #[test]
    fn empty_key_and_value_round_trip() {
        let entries = vec![record(0, b"", b""), remove(0, b"")];
        let decoded = CompressedBatch::encode(&entries, BatchCodec::Lz4).decompress();
        let entries_back: Vec<_> = InnerIter::new(decoded.clone()).collect();
        assert_eq!(entries_back.len(), 2);
        let v = find_record(&decoded, KeySpace(0), b"").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    #[should_panic(expected = "Unknown compressed-batch codec byte")]
    fn from_u8_panics_on_unknown_codec() {
        let _ = BatchCodec::from_u8(99);
    }

    #[test]
    fn from_u8_accepts_known_codec() {
        assert_eq!(BatchCodec::from_u8(1), BatchCodec::Lz4);
    }
}
