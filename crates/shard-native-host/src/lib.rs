//! Native Messaging bridge between browser extensions and the shard daemon.
//!
//! The host is spawned by Firefox/Chrome, reads length-prefixed JSON frames
//! on stdin, relays each `shard-rpc` request to the daemon's control socket
//! and writes the reply back on stdout. `Watch` requests switch the channel
//! into streaming mode until the connection ends.

pub mod manifests;
pub mod native_messaging;

pub use manifests::{Browser, HOST_NAME, install, manifest};
pub use native_messaging::{MAX_MESSAGE_BYTES, read_frame, write_frame};
