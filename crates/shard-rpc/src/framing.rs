//! Newline-delimited JSON framing shared by the daemon socket server and its
//! clients. Every message is exactly one line: serde_json never emits raw
//! newlines inside a value, so the boundary stays unambiguous.

use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn encode_frame(message: &impl Serialize) -> Vec<u8> {
    let mut frame = serde_json::to_vec(message).expect("protocol messages always serialize");
    frame.push(b'\n');
    frame
}

pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> serde_json::Result<T> {
    let body = frame
        .strip_suffix(b"\n")
        .or_else(|| frame.strip_suffix(b"\r\n"))
        .unwrap_or(frame);
    serde_json::from_slice(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newline_is_the_only_frame_delimiter() {
        let frame = encode_frame(&serde_json::json!({"nested": "line\nbreak"}));
        assert!(frame.ends_with(b"\n"));
        assert_eq!(
            decode_frame::<serde_json::Value>(&frame).unwrap()["nested"],
            "line\nbreak"
        );
    }

    #[test]
    fn crlf_is_accepted_on_read() {
        let frame = encode_frame(&serde_json::json!({"ok": true}));
        let mut crlf = frame;
        crlf.pop();
        crlf.extend_from_slice(b"\r\n");
        assert_eq!(decode_frame::<serde_json::Value>(&crlf).unwrap()["ok"], true);
    }
}