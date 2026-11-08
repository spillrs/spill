//! BEP 42 node-ID prefixes. Callers supply entropy and decide address policy.
use super::NodeId;
use std::net::IpAddr;

impl NodeId {
    /// Bind the first 21 bits to `address`, preserving the supplied random suffix.
    pub fn for_address(mut self, address: IpAddr) -> Self {
        let prefix = prefix(address, self.0[19]);
        self.0[0] = prefix[0];
        self.0[1] = prefix[1];
        self.0[2] = (prefix[2] & 0xf8) | (self.0[2] & 7);
        self
    }

    /// Local addresses are exempt. This checks the prefix, not ownership or liveness.
    pub fn valid_for(self, address: IpAddr) -> bool {
        let exempt = match address {
            IpAddr::V4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local(),
            IpAddr::V6(ip) => {
                ip.is_unique_local() || ip.is_loopback() || ip.is_unicast_link_local()
            }
        };
        let prefix = prefix(address, self.0[19]);
        exempt
            || self.0[0] == prefix[0]
                && self.0[1] == prefix[1]
                && self.0[2] & 0xf8 == prefix[2] & 0xf8
    }
}

fn prefix(address: IpAddr, random: u8) -> [u8; 4] {
    let mut bytes = [0; 8];
    let input = match address {
        IpAddr::V4(ip) => {
            bytes[..4].copy_from_slice(&(u32::from(ip) & 0x030f_3fff).to_be_bytes());
            &mut bytes[..4]
        }
        IpAddr::V6(ip) => {
            let high = u64::from_be_bytes(ip.octets()[..8].try_into().expect("eight octets"));
            bytes = (high & 0x0103_070f_1f3f_7fff).to_be_bytes();
            &mut bytes[..]
        }
    };
    input[0] |= (random & 7) << 5;
    crate::protocol::crc32c::crc32c(input).to_be_bytes()
}
