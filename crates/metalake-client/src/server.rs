//! A lake's Iceberg REST server, as a client finds it: the runtime file a running server writes once it listens, readable
//! by its user alone, which says where it is, which process it is, which version of MetaLake runs it, and the token that
//! stops it. One per lake on a machine, as Jupyter keeps one file per server.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// The version of MetaLake a server runs, which a client compares with its own: a server of another version may serve
/// another rule.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The variable a server takes its shutdown token from, so that it never shows among a process's arguments.
pub const SHUTDOWN_TOKEN: &str = "METALAKE_ICEBERG_TOKEN";

/// What a running server says of itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Runtime {
    /// What a client connects to, as `http://127.0.0.1:PORT`.
    pub url: String,
    pub port: u16,
    pub pid: u32,
    pub version: String,
    /// What a request to `POST /metalake/shutdown` bears to stop it.
    pub token: String,
}

impl Runtime {
    /// Writes it whole or not at all, readable by this user alone.
    pub fn write(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let partial = path.with_extension("partial");
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&partial)?;
        file.write_all(&serde_json::to_vec_pretty(self).map_err(|e| Error::failed(e.to_string()))?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&partial, path)?;
        Ok(())
    }

    /// What the file says, if there is one that reads.
    pub fn read(path: &Path) -> Option<Self> {
        let bytes = fs::read(path).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

/// Removes a runtime file, if it is still this server's: a server replaced meanwhile keeps its own.
pub fn remove_if_mine(path: &Path, pid: u32) {
    if Runtime::read(path).is_some_and(|runtime| runtime.pid == pid) {
        let _ = fs::remove_file(path);
    }
}

/// The directory of this user's servers' files.
pub fn directory() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| Error::failed("no home directory for the lake's server: set HOME"))?;
    Ok(PathBuf::from(home).join(".metalake").join("iceberg"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_file_round_trip() {
        let dir = std::env::temp_dir().join(format!("metalake-server-{}", uuid::Uuid::new_v4().simple()));
        let path = dir.join("lake.json");
        assert_eq!(Runtime::read(&path), None);
        let runtime = Runtime {
            url: "http://127.0.0.1:1234".into(),
            port: 1234,
            pid: 42,
            version: VERSION.into(),
            token: "t".into(),
        };
        runtime.write(&path).unwrap();
        assert_eq!(Runtime::read(&path), Some(runtime.clone()));
        assert!(!path.with_extension("partial").exists());

        remove_if_mine(&path, 7);
        assert!(path.exists(), "another server's file stays");
        remove_if_mine(&path, 42);
        assert!(!path.exists());

        fs::write(&path, "not json").unwrap();
        assert_eq!(Runtime::read(&path), None, "a file that does not read is no server");
        let _ = fs::remove_dir_all(dir);
    }
}
