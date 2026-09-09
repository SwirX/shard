/// Wire protocol version. Bump on any breaking change to messages or framing;
/// peers must reject unknown major versions before reading anything else.
pub const PROTOCOL_VERSION: u32 = 1;

pub mod framing;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    Ping,
    Start {
        url: String,
        #[serde(default)]
        dest: Option<String>,
        #[serde(default)]
        connections: Option<u32>,
    },
    List,
    Status {
        id: String,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Cancel {
        id: String,
    },
    /// Subscribe to `ServerEvent::Snapshot` broadcasts until the connection
    /// closes.
    Watch,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Pong,
    Started {
        id: String,
    },
    List {
        downloads: Vec<DownloadInfo>,
    },
    Status {
        download: Option<DownloadInfo>,
    },
    /// Control request accepted for the given download.
    Ack {
        id: String,
    },
    Error {
        code: String,
        message: String,
    },
}

/// State the daemon pushes to `Watch` subscribers. The entries mirror the
/// registry rows so the GUI and extension never talk to the engine directly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    Snapshot { downloads: Vec<DownloadInfo> },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DownloadInfo {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub dest: String,
    pub status: DownloadStatus,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub done_bytes: u64,
    #[serde(default)]
    pub sha256: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    Downloading,
    Paused,
    Completed,
    Cancelled,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{decode_frame, encode_frame};

    #[test]
    fn protocol_version_is_one() {
        assert_eq!(PROTOCOL_VERSION, 1);
    }

    #[test]
    fn requests_round_trip_through_frames() {
        let requests = [
            Request::Ping,
            Request::Start {
                url: "https://example.com/f.bin".into(),
                dest: Some("/tmp/f.bin".into()),
                connections: Some(8),
            },
            Request::Start {
                url: "https://example.com/g.bin".into(),
                dest: None,
                connections: None,
            },
            Request::List,
            Request::Status { id: "1a2b".into() },
            Request::Pause { id: "1a2b".into() },
            Request::Resume { id: "1a2b".into() },
            Request::Cancel { id: "1a2b".into() },
            Request::Watch,
        ];
        for request in requests {
            let frame = encode_frame(&request);
            assert_eq!(frame.last(), Some(&b'\n'));
            assert_eq!(decode_frame::<Request>(&frame).unwrap(), request);
        }
    }

    #[test]
    fn responses_and_events_round_trip() {
        let info = DownloadInfo {
            id: "1a2b".into(),
            url: "https://example.com/f.bin".into(),
            dest: "/tmp/f.bin".into(),
            status: DownloadStatus::Downloading,
            size: 1024,
            done_bytes: 512,
            sha256: String::new(),
            error: None,
        };
        for response in [
            Response::Pong,
            Response::Started { id: "1a2b".into() },
            Response::List {
                downloads: vec![info.clone()],
            },
            Response::Status {
                download: Some(info.clone()),
            },
            Response::Ack { id: "1a2b".into() },
            Response::Error {
                code: "not_found".into(),
                message: "no such download".into(),
            },
        ] {
            assert_eq!(
                decode_frame::<Response>(&encode_frame(&response)).unwrap(),
                response
            );
        }
        let event = ServerEvent::Snapshot {
            downloads: vec![info],
        };
        assert_eq!(
            decode_frame::<ServerEvent>(&encode_frame(&event)).unwrap(),
            event
        );
    }
}
