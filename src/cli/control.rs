use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

#[derive(Clone, Copy)]
pub enum ControlCommand {
    Pause,
    Resume,
    Cancel,
    Status,
}

impl ControlCommand {
    const fn wire(self) -> &'static str {
        match self {
            ControlCommand::Pause => "pause",
            ControlCommand::Resume => "resume",
            ControlCommand::Cancel => "cancel",
            ControlCommand::Status => "status",
        }
    }
}

pub fn send(socket: &Path, command: ControlCommand) -> std::io::Result<String> {
    let mut stream = UnixStream::connect(socket).map_err(|err| {
        std::io::Error::other(format!(
            "no live process for {} ({}); the entry may be stale or dead",
            socket.display(),
            err
        ))
    })?;
    stream.write_all(format!("{}\n", command.wire()).as_bytes())?;
    stream.flush()?;
    let mut reply = String::new();
    BufReader::new(&stream).read_line(&mut reply)?;
    Ok(reply.trim().to_string())
}

pub fn probe(socket: &Path) -> Option<String> {
    send(socket, ControlCommand::Status).ok()
}