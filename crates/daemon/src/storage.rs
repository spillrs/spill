//! Remove only the payload paths recorded in a destination binding.
use engine::provisioning::{DestinationBinding, NamedDestination};
use std::{fs::File, io, path::Path};

pub fn remove_payload(output: &Path, binding: &DestinationBinding) -> io::Result<()> {
    NamedDestination::remove(File::open(output)?, binding)
}
