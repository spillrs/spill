//! Message Stream Encryption negotiation, without clocks, entropy or I/O.
//!
//! MSE is legacy traffic obfuscation, not authenticated encryption. Private keys
//! and padding come from the caller. A failed negotiation must be discarded.

use crypto_bigint::{U768, const_monty_params, modular::ConstMontyForm};
use rc4::{KeyInit, StreamCipher};
use sha1::{Digest, Sha1};

const PRIME: &str = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563";
const P: U768 = U768::from_be_hex(PRIME);
const_monty_params!(Modulus, U768, PRIME);
const MAX_PADDING: usize = 512;

/// The selected application-stream encoding. Negotiation itself is encrypted
/// regardless of this selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Plaintext = 1,
    Rc4 = 2,
}

/// Methods offered by an initiator or accepted by a responder.
#[derive(Clone, Copy, Debug)]
pub enum Methods {
    /// Negotiate a plaintext application stream; distinct from a non-MSE connection.
    Plaintext = 1,
    Rc4 = 2,
    Both = 3,
}

/// Invalid remote negotiation data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid MSE DH public value")]
    PublicKey,
    #[error("MSE synchronization marker not found")]
    Sync,
    #[error("MSE verification constant mismatch")]
    Verification,
    #[error("invalid MSE encryption method")]
    Method,
    #[error("MSE padding exceeds 512 bytes")]
    Padding,
    #[error("unknown MSE torrent")]
    UnknownTorrent,
    #[error("truncated MSE negotiation")]
    Truncated,
}

/// One direction's RC4 state, including the mandatory 1,024-byte discard.
/// Never restart it at a message boundary.
pub struct Cipher(rc4::Rc4);

impl Cipher {
    fn new(key: [u8; 20]) -> Self {
        let mut cipher = Self(rc4::Rc4::new_from_slice(&key).expect("SHA-1 RC4 key"));
        cipher.apply(&mut [0; 1024]);
        cipher
    }

    /// Encrypt or decrypt the next bytes in this direction.
    pub fn apply(&mut self, bytes: &mut [u8]) {
        self.0.apply_keystream(bytes);
    }
}

/// State transferred from negotiation to the ordinary peer stream.
pub struct Negotiated {
    pub method: Method,
    pub encrypt: Cipher,
    pub decrypt: Cipher,
    /// Already-decoded initial application data, before `surplus`.
    pub initial: Vec<u8>,
    /// Raw transport bytes beyond negotiation. Decode according to `method`.
    pub surplus: Vec<u8>,
    /// The wire key used for this negotiation, including hybrid/v2 routing.
    pub wire_key: [u8; 20],
}

/// Bytes to transmit and, when complete, ownership of the application stream.
pub struct Progress {
    pub output: Vec<u8>,
    pub negotiated: Option<Negotiated>,
}

#[derive(Clone, Copy)]
enum Phase {
    Public,
    RequestSync,
    Torrent,
    ResponseSync,
    Offer,
    Selection,
    Padding(usize),
    Initial(usize),
    Done,
}

/// Incremental MSE initiator or responder. Call `receive` with newly received
/// bytes; transmit each returned `output` before waiting for more input. Stop
/// using the codec once it returns `negotiated` or an error. Map transport EOF
/// before completion to [`Error::Truncated`].
pub struct Negotiation {
    initiator: bool,
    phase: Phase,
    private: [u8; 20],
    padding: Vec<u8>,
    methods: Methods,
    wire_key: Option<[u8; 20]>,
    secret: [u8; 96],
    encrypt: Option<Cipher>,
    decrypt: Option<Cipher>,
    marker: Vec<u8>,
    method: Option<Method>,
    input: Vec<u8>,
    initial: Vec<u8>,
}

impl Negotiation {
    /// Start an outgoing negotiation. `initial` is encrypted application data,
    /// normally the standard BitTorrent handshake. Caller-built padding is at
    /// most 512 bytes and application data at most 65,535 bytes.
    pub fn initiator(
        private: [u8; 20],
        padding: &[u8],
        wire_key: [u8; 20],
        methods: Methods,
        initial: &[u8],
    ) -> (Self, Vec<u8>) {
        assert!(initial.len() <= u16::MAX as usize);
        let mut codec = Self::responder(private, methods, padding);
        codec.initiator = true;
        codec.wire_key = Some(wire_key);
        codec.initial = initial.to_vec();
        let mut output = public_key(&private).to_vec();
        output.extend_from_slice(padding);
        (codec, output)
    }

    /// Prepare an incoming negotiation. No bytes are sent until the peer's
    /// public value has been received and validated.
    pub fn responder(private: [u8; 20], methods: Methods, padding: &[u8]) -> Self {
        assert!(padding.len() <= MAX_PADDING);
        Self {
            initiator: false,
            phase: Phase::Public,
            private,
            padding: padding.to_vec(),
            methods,
            wire_key: None,
            secret: [0; 96],
            encrypt: None,
            decrypt: None,
            marker: Vec::new(),
            method: None,
            input: Vec::new(),
            initial: Vec::new(),
        }
    }

    /// Consume newly arrived bytes. The responder's lookup maps
    /// `SHA1("req2" || wire_key)` to a registered wire key. It is never called
    /// by the initiator. Surplus input is returned unchanged on completion.
    pub fn receive(
        &mut self,
        bytes: &[u8],
        mut lookup: impl FnMut([u8; 20]) -> Option<[u8; 20]>,
    ) -> Result<Progress, Error> {
        assert!(!matches!(self.phase, Phase::Done), "completed MSE codec");
        self.input.extend_from_slice(bytes);
        let mut output = Vec::new();
        loop {
            match self.phase {
                Phase::Public => {
                    if self.input.len() < 96 {
                        break;
                    }
                    let remote = U768::from_be_slice(&self.take(96));
                    if remote < U768::from_u8(2) || remote > P.wrapping_sub(&U768::from_u8(2)) {
                        return Err(Error::PublicKey);
                    }
                    self.secret = power(remote, &self.private);
                    if self.initiator {
                        let key = self.wire_key.expect("initiator wire key");
                        self.init_ciphers(key);
                        output.extend_from_slice(&hash(&[b"req1", &self.secret]));
                        let mut request = torrent_key(key);
                        let mask = hash(&[b"req3", &self.secret]);
                        for (byte, mask) in request.iter_mut().zip(mask) {
                            *byte ^= mask;
                        }
                        output.extend_from_slice(&request);
                        let mut header = vec![0; 8];
                        header.extend_from_slice(&(self.methods as u32).to_be_bytes());
                        header.extend_from_slice(&0_u16.to_be_bytes());
                        header.extend_from_slice(&(self.initial.len() as u16).to_be_bytes());
                        header.append(&mut self.initial);
                        self.encrypt
                            .as_mut()
                            .expect("outgoing key")
                            .apply(&mut header);
                        output.extend(header);
                        self.marker = vec![0; 8];
                        self.decrypt
                            .as_mut()
                            .expect("incoming key")
                            .apply(&mut self.marker);
                        self.phase = Phase::ResponseSync;
                    } else {
                        output.extend_from_slice(&public_key(&self.private));
                        output.extend_from_slice(&self.padding);
                        self.marker = hash(&[b"req1", &self.secret]).to_vec();
                        self.phase = Phase::RequestSync;
                    }
                }
                Phase::RequestSync | Phase::ResponseSync => {
                    if !self.sync()? {
                        break;
                    }
                    self.phase = if self.initiator {
                        Phase::Selection
                    } else {
                        Phase::Torrent
                    };
                }
                Phase::Torrent => {
                    if self.input.len() < 20 {
                        break;
                    }
                    let mut request: [u8; 20] = self.take(20).try_into().expect("req2 size");
                    let mask = hash(&[b"req3", &self.secret]);
                    for (byte, mask) in request.iter_mut().zip(mask) {
                        *byte ^= mask;
                    }
                    let key = lookup(request).ok_or(Error::UnknownTorrent)?;
                    self.wire_key = Some(key);
                    self.init_ciphers(key);
                    self.phase = Phase::Offer;
                }
                Phase::Offer | Phase::Selection => {
                    let count = if self.initiator { 6 } else { 14 };
                    if self.input.len() < count {
                        break;
                    }
                    let header = self.decode(count);
                    let fields = if self.initiator {
                        &header[..]
                    } else {
                        if header[..8] != [0; 8] {
                            return Err(Error::Verification);
                        }
                        &header[8..]
                    };
                    let bits = u32::from_be_bytes(fields[..4].try_into().expect("method size"));
                    let accepted = bits & self.methods as u32;
                    self.method = Some(if self.initiator {
                        match bits {
                            1 if accepted == 1 => Method::Plaintext,
                            2 if accepted == 2 => Method::Rc4,
                            _ => return Err(Error::Method),
                        }
                    } else if accepted & 2 != 0 {
                        Method::Rc4
                    } else if accepted & 1 != 0 {
                        Method::Plaintext
                    } else {
                        return Err(Error::Method);
                    });
                    let padding =
                        u16::from_be_bytes(fields[4..6].try_into().expect("padding size")) as usize;
                    if padding > MAX_PADDING {
                        return Err(Error::Padding);
                    }
                    self.phase = Phase::Padding(padding);
                }
                Phase::Padding(padding) => {
                    let count = padding + if self.initiator { 0 } else { 2 };
                    if self.input.len() < count {
                        break;
                    }
                    let decoded = self.decode(count);
                    if self.initiator {
                        return Ok(self.complete(output));
                    }
                    let length =
                        u16::from_be_bytes(decoded[padding..].try_into().expect("IA size"));
                    self.phase = Phase::Initial(length as usize);
                }
                Phase::Initial(length) => {
                    if self.input.len() < length {
                        break;
                    }
                    self.initial = self.decode(length);
                    let mut reply = vec![0; 8];
                    reply.extend_from_slice(
                        &(self.method.expect("selected method") as u32).to_be_bytes(),
                    );
                    reply.extend_from_slice(&0_u16.to_be_bytes());
                    self.encrypt
                        .as_mut()
                        .expect("outgoing key")
                        .apply(&mut reply);
                    output.extend(reply);
                    return Ok(self.complete(output));
                }
                Phase::Done => unreachable!("completed MSE codec"),
            }
        }
        Ok(Progress {
            output,
            negotiated: None,
        })
    }

    fn take(&mut self, count: usize) -> Vec<u8> {
        self.input.drain(..count).collect()
    }

    fn decode(&mut self, count: usize) -> Vec<u8> {
        let mut bytes = self.take(count);
        self.decrypt
            .as_mut()
            .expect("incoming key")
            .apply(&mut bytes);
        bytes
    }

    fn sync(&mut self) -> Result<bool, Error> {
        let window = self.input.len().min(MAX_PADDING + self.marker.len());
        if let Some(offset) = self.input[..window]
            .windows(self.marker.len())
            .position(|bytes| bytes == self.marker)
        {
            self.input.drain(..offset + self.marker.len());
            return Ok(true);
        }
        if self.input.len() >= MAX_PADDING + self.marker.len() {
            return Err(Error::Sync);
        }
        Ok(false)
    }

    fn init_ciphers(&mut self, wire_key: [u8; 20]) {
        let a = Cipher::new(hash(&[b"keyA", &self.secret, &wire_key]));
        let b = Cipher::new(hash(&[b"keyB", &self.secret, &wire_key]));
        let (encrypt, decrypt) = if self.initiator { (a, b) } else { (b, a) };
        self.encrypt = Some(encrypt);
        self.decrypt = Some(decrypt);
    }

    fn complete(&mut self, output: Vec<u8>) -> Progress {
        self.phase = Phase::Done;
        Progress {
            output,
            negotiated: Some(Negotiated {
                method: self.method.expect("selected method"),
                encrypt: self.encrypt.take().expect("outgoing key"),
                decrypt: self.decrypt.take().expect("incoming key"),
                initial: std::mem::take(&mut self.initial),
                surplus: std::mem::take(&mut self.input),
                wire_key: self.wire_key.expect("resolved wire key"),
            }),
        }
    }
}

/// Lookup key for incoming MSE routing; computed once per registered wire key.
pub fn torrent_key(wire_key: [u8; 20]) -> [u8; 20] {
    hash(&[b"req2", &wire_key])
}

fn hash(parts: &[&[u8]]) -> [u8; 20] {
    let mut hash = Sha1::new();
    for part in parts {
        hash.update(part);
    }
    hash.finalize().into()
}

fn public_key(private: &[u8; 20]) -> [u8; 96] {
    power(U768::from_u8(2), private)
}

fn power(base: U768, private: &[u8; 20]) -> [u8; 96] {
    let mut exponent = [0; 96];
    exponent[76..].copy_from_slice(private);
    let exponent = U768::from_be_slice(&exponent);
    ConstMontyForm::<Modulus, { U768::LIMBS }>::new(&base)
        .pow_bounded_exp(&exponent, 160)
        .retrieve()
        .to_be_bytes()
        .into()
}
