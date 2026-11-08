pub(super) fn crc32c(input: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in input {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
