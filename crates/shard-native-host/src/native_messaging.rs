//! WebExtension native messaging framing.
//!
//! The browser spawns the host and pipes messages in/out, framing each one
//! as `u32` length (native endian) followed by that many bytes of JSON
//! (see the MDN "Native messaging" reference). We reuse the same framing as
//! the daemon: messages are precisely the `shard-rpc` JSON values.

use std::io::{self, Read, Write};

/// Largest message we will accept from the browser (1 MiB). The daemon's own
/// frames are far smaller; the cap guards against a misbehaving peer.
pub const MAX_MESSAGE_BYTES: u32 = 1 << 20;

/// Read one native-messaging frame. `Ok(None)` at a clean EOF means the
/// browser closed the channel; any other read failure is an error.
pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; 4];
    if let Err(err) = reader.read_exact(&mut header) {
        return if err.kind() == io::ErrorKind::UnexpectedEof {
            Ok(None)
        } else {
            Err(err)
        };
    }
    let length = u32::from_ne_bytes(header);
    if length > MAX_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("native message too large ({length} bytes)"),
        ));
    }
    let mut payload = vec![0u8; length as usize];
    reader.read_exact(&mut payload)?;
    Ok(Some(payload))
}

/// Write one native-messaging frame to `writer`.
pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> io::Result<()> {
    let length = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "payload too large for native messaging",
        )
    })?;
    if length > MAX_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("payload too large ({length} bytes)"),
        ));
    }
    writer.write_all(&length.to_ne_bytes())?;
    writer.write_all(payload)?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_through_a_cursor() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        write_frame(&mut cursor, br#"{"hello":"world"}"#).unwrap();
        let mut read = std::io::Cursor::new(cursor.into_inner());
        assert_eq!(
            read_frame(&mut read).unwrap(),
            Some(br#"{"hello":"world"}"#.to_vec())
        );
    }

    #[test]
    fn clean_eof_reads_as_no_message() {
        let mut empty = std::io::Cursor::new(Vec::new());
        assert_eq!(read_frame(&mut empty).unwrap(), None);
    }

    #[test]
    fn overlarge_payload_is_rejected() {
        let huge = vec![0u8; (MAX_MESSAGE_BYTES + 1) as usize];
        let mut cursor = std::io::Cursor::new(Vec::new());
        assert!(write_frame(&mut cursor, &huge).is_err());

        let mut cursor = std::io::Cursor::new(Vec::new());
        cursor
            .write_all(&(MAX_MESSAGE_BYTES + 1).to_ne_bytes())
            .unwrap();
        let mut read = std::io::Cursor::new(cursor.into_inner());
        assert!(read_frame(&mut read).is_err());
    }
}
