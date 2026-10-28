use super::{Error, ErrorKind, Limits, Node, Value};

/// Encode a semantic value canonically within the output/node/depth budgets.
///
/// Dictionaries are byte-sorted and unique by construction. Original decoded
/// spans are ignored: callers hashing metainfo must use `Node::raw_bytes`.
/// `max_input_len` is a decoder-only budget.
pub fn encode(node: &Node<'_>, limits: Limits) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder {
        limits: limits.validate()?,
        output: Vec::new(),
        nodes: 0,
    };
    encoder.value(node, 0)?;
    Ok(encoder.output)
}

struct Encoder {
    limits: Limits,
    output: Vec<u8>,
    nodes: usize,
}

impl Encoder {
    fn error(&self, kind: ErrorKind) -> Error {
        Error::new(self.output.len(), kind)
    }

    fn count_node(&mut self) -> Result<(), Error> {
        if self.nodes >= self.limits.max_nodes {
            return Err(self.error(ErrorKind::NodeLimit));
        }
        self.nodes += 1;
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self
            .output
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > self.limits.max_output_len)
        {
            return Err(self.error(ErrorKind::OutputLimit));
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }

    fn string(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() > self.limits.max_string_len || u32::try_from(bytes.len()).is_err() {
            return Err(self.error(ErrorKind::StringLimit));
        }
        self.append(bytes.len().to_string().as_bytes())?;
        self.append(b":")?;
        self.append(bytes)
    }

    fn value(&mut self, node: &Node<'_>, depth: usize) -> Result<(), Error> {
        self.count_node()?;
        match node.value() {
            Value::Integer(value) => {
                self.append(b"i")?;
                self.append(value.to_string().as_bytes())?;
                self.append(b"e")
            }
            Value::Bytes(bytes) => self.string(bytes),
            Value::List(_) | Value::Dictionary(_) if depth >= self.limits.max_depth => {
                Err(self.error(ErrorKind::DepthLimit))
            }
            Value::List(entries) => {
                self.append(b"l")?;
                for child in entries {
                    self.value(child, depth + 1)?;
                }
                self.append(b"e")
            }
            Value::Dictionary(entries) => {
                self.append(b"d")?;
                for (key, child) in entries {
                    self.count_node()?;
                    self.string(key)?;
                    self.value(child, depth + 1)?;
                }
                self.append(b"e")
            }
        }
    }
}
