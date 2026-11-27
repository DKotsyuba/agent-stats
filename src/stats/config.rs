//! Service admission settings, private state and credential validation.
use super::{Error, normalize};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder, File, OpenOptions},
    io::Read,
    net::SocketAddr,
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub(super) const SOCKET: &str = "/run/agent-stats/read.sock";

/// Fixed upper bounds keep the configured request and queue maxima composable.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Config {
    pub listen: SocketAddr,
    pub state_dir: PathBuf,
    pub socket: PathBuf,
    pub allowed_uids: Vec<u32>,
    /// Exact UIDs that may write result ratings. When absent, anyone who may read may rate.
    pub rating_uids: Option<Vec<u32>>,
    pub token_file: PathBuf,
    pub digest_key_file: PathBuf,
    pub provider_labels: BTreeMap<String, String>,
    /// Model-name prefix → provider label for custom gateways that report no base URL
    /// (e.g. `"glm-" = "zai"`); the longest matching prefix wins.
    pub model_providers: BTreeMap<String, String>,
    pub max_body_bytes: usize,
    pub max_records: usize,
    pub max_requests: usize,
    pub write_backlog: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 4318)),
            state_dir: "/var/lib/agent-stats".into(),
            socket: SOCKET.into(),
            allowed_uids: Vec::new(),
            rating_uids: None,
            token_file: "/etc/agent-stats/ingest.token".into(),
            digest_key_file: "/etc/agent-stats/digest.key".into(),
            provider_labels: BTreeMap::new(),
            model_providers: BTreeMap::new(),
            max_body_bytes: 512 * 1024,
            max_records: 512,
            max_requests: 8,
            write_backlog: 8,
        }
    }
}

impl Config {
    /// Refuse invalid limits and non-loopback listeners without echoing configuration values.
    pub(super) fn load(path: &Path) -> Result<Self, Error> {
        let input = read(
            File::open(path).map_err(|_| Error("config_open_failed"))?,
            64 * 1024,
        )
        .map_err(|_| Error("config_read_failed"))?;
        let input = std::str::from_utf8(&input).map_err(|_| Error("invalid_config"))?;
        let config: Self = toml::from_str(input).map_err(|_| Error("invalid_config"))?;
        if !config.listen.ip().is_loopback()
            || ![
                &config.state_dir,
                &config.socket,
                &config.token_file,
                &config.digest_key_file,
            ]
            .into_iter()
            .all(|path| path.is_absolute())
            || !(1..=1024 * 1024).contains(&config.max_body_bytes)
            || !(1..=1024).contains(&config.max_records)
            || !(1..=16).contains(&config.max_requests)
            || !(1..=32).contains(&config.write_backlog)
            || config
                .provider_labels
                .values()
                .chain(config.model_providers.values())
                .any(|value| !normalize::label(value))
            || config
                .model_providers
                .keys()
                .any(|prefix| !normalize::label(prefix))
        {
            return Err(Error("invalid_config"));
        }
        Ok(config)
    }
}

/// Read only a service-owned regular credential file with exactly 0600 permissions.
pub(super) fn secret(path: &Path) -> Result<Vec<u8>, Error> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)
        .map_err(|_| Error("credential_open_failed"))?;
    let metadata = file
        .metadata()
        .map_err(|_| Error("credential_open_failed"))?;
    if !metadata.is_file()
        || metadata.mode() & 0o7777 != 0o600
        || metadata.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(Error("unsafe_credential"));
    }
    let mut value = read(file, 129).map_err(|_| Error("invalid_credential"))?;
    if value.last() == Some(&b'\n') {
        value.pop();
    }
    if !(32..=128).contains(&value.len()) || !value.iter().all(u8::is_ascii_graphic) {
        return Err(Error("invalid_credential"));
    }
    Ok(value)
}

/// Read a bounded local file and reject a truncated input.
fn read(file: File, limit: usize) -> std::io::Result<Vec<u8>> {
    let mut contents = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut contents)?;
    if contents.len() > limit {
        return Err(std::io::Error::other("file too large"));
    }
    Ok(contents)
}

/// Create the private SQLite directory or verify an existing one.
pub(super) fn private_dir(path: &Path) -> Result<(), Error> {
    match DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(_) => return Err(Error("state_directory_failed")),
    }
    directory(path, true)
}

/// Require a service-owned directory. The socket parent may permit group traversal.
pub(super) fn directory(path: &Path, private: bool) -> Result<(), Error> {
    let metadata = fs::symlink_metadata(path).map_err(|_| Error("unsafe_directory"))?;
    let mode = metadata.mode();
    if !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || if private {
            mode & 0o777 != 0o700
        } else {
            mode & 0o022 != 0
        }
    {
        return Err(Error("unsafe_directory"));
    }
    Ok(())
}

/// Return whether the socket path is vacant or has a service-owned stale candidate.
pub(super) fn existing_socket(path: &Path) -> Result<bool, Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.uid() == rustix::process::geteuid().as_raw() =>
        {
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        _ => Err(Error("unsafe_socket")),
    }
}

/// Hold an advisory lock for a service-owned endpoint or database for its lifetime.
pub(super) fn lock(path: &Path) -> Result<File, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)
        .map_err(|_| Error("lock_open_failed"))?;
    if !file.metadata().is_ok_and(|metadata| {
        metadata.is_file()
            && metadata.uid() == rustix::process::geteuid().as_raw()
            && metadata.mode() & 0o777 == 0o600
    }) {
        return Err(Error("unsafe_lock"));
    }
    fs2::FileExt::try_lock_exclusive(&file).map_err(|_| Error("already_running"))?;
    Ok(file)
}
