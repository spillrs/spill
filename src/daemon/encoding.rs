use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Write, io};

pub(crate) fn hex_key<const N: usize>(key: [u8; N]) -> String {
    let mut hex = String::with_capacity(N * 2);
    for byte in key {
        write!(hex, "{byte:02x}").unwrap();
    }
    hex
}

pub(crate) fn decode_key<const N: usize>(text: &str) -> io::Result<[u8; N]> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid catalog hex key");
    if text.len() != N * 2 || !text.is_ascii() {
        return Err(invalid());
    }
    let mut key = [0; N];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(key)
}

pub(crate) fn encode_records<T: Serialize>(
    records: &[([u8; 20], T)],
) -> serde_json::Result<String> {
    serde_json::to_string(
        &records
            .iter()
            .map(|(key, value)| (hex_key(*key), value))
            .collect::<Vec<_>>(),
    )
}

pub(crate) fn decode_records<T: DeserializeOwned>(text: &str) -> io::Result<Vec<([u8; 20], T)>> {
    let records: Vec<(String, T)> = serde_json::from_str(text)?;
    records
        .into_iter()
        .map(|(text, value)| Ok((decode_key(&text)?, value)))
        .collect()
}
