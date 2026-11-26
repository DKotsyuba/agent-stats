//! One bounded JSON line per Unix-socket request, admitted by kernel peer UID.
use super::{
    Error, State,
    api::{self, Operation, Request},
    config, now_ns, store,
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    time::timeout,
};

const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_REQUEST: u64 = 4096;
const MAX_RESPONSE: u64 = 1024 * 1024;

/// Own the published socket inode until the listener finishes.
pub(super) struct Endpoint {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|meta| meta.dev() == self.device && meta.ino() == self.inode)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Bind under a trusted directory and recover only an owned, demonstrably stale socket.
pub(super) async fn bind(path: &Path) -> Result<(UnixListener, Endpoint), Error> {
    config::directory(path.parent().ok_or(Error("unsafe_socket"))?, false)?;
    let lock = config::lock(&path.with_extension("lock"))?;
    if config::existing_socket(path)? {
        match timeout(IO_TIMEOUT, UnixStream::connect(path)).await {
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                fs::remove_file(path).map_err(|_| Error("socket_cleanup_failed"))?;
            }
            _ => return Err(Error("socket_in_use")),
        }
    }
    let listener = UnixListener::bind(path).map_err(|_| Error("socket_bind_failed"))?;
    let meta = fs::symlink_metadata(path).map_err(|_| Error("socket_bind_failed"))?;
    let endpoint = Endpoint {
        path: path.into(),
        device: meta.dev(),
        inode: meta.ino(),
        _lock: lock,
    };
    fs::set_permissions(path, fs::Permissions::from_mode(0o660))
        .map_err(|_| Error("socket_permissions_failed"))?;
    Ok((listener, endpoint))
}

/// Admit only configured UIDs and the service user, before any request bytes are read.
pub(super) fn authorized(stream: &UnixStream, allowed: &[u32]) -> bool {
    stream
        .peer_cred()
        .is_ok_and(|peer| allowed_uid(rustix::process::geteuid().as_raw(), allowed, peer.uid()))
}

/// Apply the service owner and explicitly configured UID policy.
fn allowed_uid(owner: u32, allowed: &[u32], peer: u32) -> bool {
    owner == peer || allowed.contains(&peer)
}

/// Process one admitted connection with independently bounded frame and write times.
pub(super) async fn connection(mut stream: UnixStream, state: Arc<State>) {
    let result = match timeout(IO_TIMEOUT, line(&mut stream, MAX_REQUEST)).await {
        Ok(Ok(input)) => dispatch(&input, state).await,
        _ => Err(Error("invalid_request")),
    };
    let reply = match result {
        Ok(data) => json!({"version": 1, "ok": true, "data": data}),
        Err(error) => json!({"version": 1, "ok": false, "error": error.0}),
    };
    if let Ok(mut bytes) = serde_json::to_vec(&reply) {
        if bytes.len() as u64 >= MAX_RESPONSE {
            bytes = br#"{"version":1,"ok":false,"error":"query_too_large"}"#.to_vec();
        }
        bytes.push(b'\n');
        let _ = timeout(IO_TIMEOUT, stream.write_all(&bytes)).await;
    }
}

/// Read one LF-terminated frame without allocating past its byte limit.
async fn line(stream: &mut UnixStream, limit: u64) -> Result<Vec<u8>, Error> {
    let mut frame = Vec::new();
    BufReader::new(stream.take(limit))
        .read_until(b'\n', &mut frame)
        .await
        .map_err(|_| Error("socket_read_failed"))?;
    if frame.pop() != Some(b'\n') {
        return Err(Error("invalid_frame"));
    }
    Ok(frame)
}

/// Move the read permit into blocking work so cancellation cannot free capacity early.
async fn dispatch(input: &[u8], state: Arc<State>) -> Result<Value, Error> {
    let request: Request = serde_json::from_slice(input).map_err(|_| Error("invalid_request"))?;
    if request.version != 1 {
        return Err(Error("unsupported_version"));
    }
    let permit = state
        .reads
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error("busy"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let conn = store::reader(&state.database)?;
        api::read(&conn, request.operation, now_ns(), &state.counters)
    })
    .await
    .map_err(|_| Error("read_failed"))?
}

/// Use the same versioned protocol as Hub clients and return the full success envelope.
pub(crate) async fn call(path: &Path, operation: Operation) -> Result<Value, Error> {
    timeout(IO_TIMEOUT, async {
        let mut stream = UnixStream::connect(path)
            .await
            .map_err(|_| Error("socket_unavailable"))?;
        let mut frame = serde_json::to_vec(&Request {
            version: 1,
            operation,
        })
        .map_err(|_| Error("invalid_request"))?;
        frame.push(b'\n');
        if frame.len() as u64 > MAX_REQUEST {
            return Err(Error("invalid_request"));
        }
        stream
            .write_all(&frame)
            .await
            .map_err(|_| Error("socket_write_failed"))?;
        let bytes = line(&mut stream, MAX_RESPONSE).await?;
        let reply: Value = serde_json::from_slice(&bytes).map_err(|_| Error("invalid_response"))?;
        if reply["version"].as_u64() != Some(1) || reply["ok"].as_bool().is_none() {
            return Err(Error("invalid_response"));
        }
        Ok(reply)
    })
    .await
    .map_err(|_| Error("socket_timeout"))?
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    reason = "Test setup failures should include their cause"
)]
mod tests {
    use super::*;

    /// Exercise the kernel credential lookup and policy for owner, allowed and other UIDs.
    #[tokio::test]
    async fn kernel_peer_uid_policy() {
        let (peer, _other_end) = UnixStream::pair().expect("socket pair");
        let uid = peer.peer_cred().expect("kernel peer credentials").uid();
        let other = uid ^ 1;
        assert!(authorized(&peer, &[]));
        assert!(!allowed_uid(other, &[], uid));
        assert!(!allowed_uid(other, &[other ^ 2], uid));
        assert!(allowed_uid(other, &[uid], uid));
    }
}
