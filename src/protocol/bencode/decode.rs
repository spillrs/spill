use std::collections::BTreeMap;

use super::{Error, ErrorKind, Limits, Node, Value};

/// Decode exactly one value. Trailing bytes are an error.
pub fn decode(input: &[u8], limits: Limits) -> Result<Node<'_>, Error> {
    let (node, consumed) = decode_prefix(input, limits)?;
    if consumed != input.len() {
        return Err(Error::new(consumed, ErrorKind::TrailingBytes));
    }
    Ok(node)
}

/// Decode one value at the start of input and return the consumed length.
///
/// Useful for a bencoded metadata header followed by opaque payload bytes. The
/// input budget applies to the entire supplied slice, including trailing data.
pub fn decode_prefix(input: &[u8], limits: Limits) -> Result<(Node<'_>, usize), Error> {
    let limits = limits.validate()?;
    if input.len() > limits.max_input_len {
        return Err(Error::new(0, ErrorKind::InputLimit));
    }
    let mut parser = Parser {
        input,
        limits,
        position: 0,
        nodes: 0,
    };
    let node = parser.value(0)?;
    Ok((node, parser.position))
}

struct Parser<'a> {
    input: &'a [u8],
    limits: Limits,
    position: usize,
    nodes: usize,
}

impl<'a> Parser<'a> {
    fn error(&self, kind: ErrorKind) -> Error {
        Error::new(self.position, kind)
    }

    fn peek(&self) -> Result<u8, Error> {
        self.input
            .get(self.position)
            .copied()
            .ok_or_else(|| self.error(ErrorKind::UnexpectedEnd))
    }

    fn count_node(&mut self) -> Result<(), Error> {
        if self.nodes >= self.limits.max_nodes {
            return Err(self.error(ErrorKind::NodeLimit));
        }
        self.nodes += 1;
        Ok(())
    }

    fn value(&mut self, depth: usize) -> Result<Node<'a>, Error> {
        self.count_node()?;
        let start = self.position;
        let value = match self.peek()? {
            b'i' => Value::Integer(self.integer()?),
            b'0'..=b'9' => Value::Bytes(self.string()?),
            b'l' | b'd' if depth >= self.limits.max_depth => {
                return Err(self.error(ErrorKind::DepthLimit));
            }
            b'l' => {
                self.position += 1;
                let mut entries = Vec::new();
                while self.peek()? != b'e' {
                    entries.push(self.value(depth + 1)?);
                }
                self.position += 1;
                Value::List(entries)
            }
            b'd' => {
                self.position += 1;
                let mut entries = BTreeMap::new();
                while self.peek()? != b'e' {
                    self.count_node()?;
                    if !self.peek()?.is_ascii_digit() {
                        return Err(self.error(ErrorKind::DictionaryKey));
                    }
                    let key = self.string()?;
                    let value = self.value(depth + 1)?;
                    entries.insert(key, value);
                }
                self.position += 1;
                Value::Dictionary(entries)
            }
            _ => return Err(self.error(ErrorKind::InvalidToken)),
        };
        Ok(Node {
            value,
            raw: Some(&self.input[start..self.position]),
        })
    }

    fn integer(&mut self) -> Result<i64, Error> {
        self.position += 1; // 'i'
        let start = self.position;
        let negative = self.peek()? == b'-';
        if negative {
            self.position += 1;
        }
        let digits = self.position;
        while self.peek()?.is_ascii_digit() {
            self.position += 1;
        }
        if self.peek()? != b'e' || self.position == digits {
            return Err(self.error(ErrorKind::InvalidInteger));
        }
        if self.input[digits] == b'0' && (negative || self.position - digits > 1) {
            return Err(Error::new(start, ErrorKind::InvalidInteger));
        }
        // Only ASCII '-' and digits reached this range. std parsing performs
        // checked signed conversion, including the asymmetric i64::MIN bound.
        let text = std::str::from_utf8(&self.input[start..self.position])
            .map_err(|_| self.error(ErrorKind::InvalidInteger))?;
        let value = text
            .parse::<i64>()
            .map_err(|_| Error::new(start, ErrorKind::IntegerOverflow))?;
        self.position += 1; // 'e'
        Ok(value)
    }

    fn string(&mut self) -> Result<&'a [u8], Error> {
        let start = self.position;
        while self.peek()?.is_ascii_digit() {
            self.position += 1;
        }
        if self.position == start || self.peek()? != b':' {
            return Err(self.error(ErrorKind::InvalidStringLength));
        }
        let text = std::str::from_utf8(&self.input[start..self.position])
            .map_err(|_| self.error(ErrorKind::InvalidStringLength))?;
        // String lengths are bounded to u32. Independently configured
        // string/input budgets remain much smaller by default.
        let length = text
            .parse::<u32>()
            .map_err(|_| Error::new(start, ErrorKind::StringLengthOverflow))?
            as usize;
        if length > self.limits.max_string_len {
            return Err(Error::new(start, ErrorKind::StringLimit));
        }
        self.position += 1; // ':'
        let end = self
            .position
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
            .ok_or_else(|| self.error(ErrorKind::UnexpectedEnd))?;
        let bytes = &self.input[self.position..end];
        self.position = end;
        Ok(bytes)
    }
}
