//! CRC-32C (Castagnoli).
//!
//! Written out rather than pulled in. It is thirty lines, it has a published
//! test vector, and the alternative is a dependency on the write path of a
//! system whose entire claim is that its durability boundary is small enough to
//! reason about.
//!
//! Castagnoli specifically, because Go's standard library has it
//! (`hash/crc32.Castagnoli`) and the Go reader must agree with these bytes
//! without anyone porting a table.
//!
//! Table-driven, one byte at a time: roughly a gigabyte a second, against a
//! write path that moves kilobytes. The hardware instruction would need
//! `unsafe`, and this crate forbids it.

/// The reversed Castagnoli polynomial.
const POLY: u32 = 0x82F6_3B78;

/// Byte-at-a-time lookup table, built at compile time.
static TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        #[expect(clippy::cast_possible_truncation, reason = "i < 256 by the loop bound")]
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Checksum of `bytes`.
#[must_use]
pub fn checksum(bytes: &[u8]) -> u32 {
    update(0, bytes)
}

/// Continue a checksum over another slice.
///
/// The frame header and the payload live in different buffers at the moment
/// they are checksummed, and copying them together first to get one slice would
/// be a memcpy per record to satisfy a signature.
#[must_use]
pub fn update(crc: u32, bytes: &[u8]) -> u32 {
    let mut crc = !crc;
    for &b in bytes {
        #[expect(
            clippy::cast_possible_truncation,
            reason = "taking the low byte is what the algorithm does; the high \
                      bits are shifted in on the next line"
        )]
        let low = crc as u8;
        crc = (crc >> 8) ^ TABLE[usize::from(low ^ b)];
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_published_check_value() {
        // RFC 3720 / iSCSI: CRC-32C of "123456789" is 0xE3069283. If this
        // passes, Go's hash/crc32 Castagnoli table agrees with ours.
        assert_eq!(checksum(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn empty_input_is_zero() {
        assert_eq!(checksum(b""), 0);
    }

    #[test]
    fn continuing_a_checksum_equals_checksumming_the_whole() {
        // The property the two-buffer write path depends on.
        let all = b"SENTJRN\x01the quick brown fox";
        for split in 0..all.len() {
            let (a, b) = all.split_at(split);
            assert_eq!(update(checksum(a), b), checksum(all), "split at {split}");
        }
    }

    #[test]
    fn a_single_flipped_bit_changes_the_checksum() {
        let mut bytes = *b"a record that matters";
        let before = checksum(&bytes);
        for i in 0..bytes.len() {
            for bit in 0..8u8 {
                bytes[i] ^= 1 << bit;
                assert_ne!(checksum(&bytes), before, "byte {i} bit {bit}");
                bytes[i] ^= 1 << bit;
            }
        }
    }

    #[test]
    fn length_is_part_of_the_value() {
        // Trailing zeros must not be invisible: a frame truncated to a shorter
        // run of zeros has to fail its check.
        assert_ne!(checksum(b"\0"), checksum(b"\0\0"));
        assert_ne!(checksum(b""), checksum(b"\0"));
    }
}
