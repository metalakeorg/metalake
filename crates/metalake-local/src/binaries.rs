//! Where Postgres's binaries are: one copy per machine, shared by every lake. Who gets them is apart from what is done
//! with them, so a package that ships its own only has to say where they are.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;

use postgresql_archive::VersionReq;
use postgresql_archive::blocking::{extract, get_archive};
use postgresql_archive::configuration::theseus::URL;

use crate::{Error, Result};

/// The major version every lake of this MetaLake runs, which a cluster cannot leave without `pg_upgrade`.
pub const MAJOR: u32 = 17;

/// Names a directory of binaries, for a package that ships them.
const ENVIRONMENT: &str = "METALAKE_POSTGRES_BIN";

#[derive(Debug, Clone)]
pub struct Binaries {
    bin: PathBuf,
}

impl Binaries {
    /// The binaries the environment names, which is how a package says it ships them; failing that, this machine's,
    /// fetched the first time, with their hash checked.
    pub fn find() -> Result<Self> {
        if let Some(bin) = env::var_os(ENVIRONMENT) {
            return Self::at(PathBuf::from(bin));
        }
        let installed = home()?.join(".metalake").join("postgres").join(MAJOR.to_string());
        if !installed.is_dir() {
            // On a thread of its own: these calls run a runtime, which cannot start inside a caller's.
            let target = installed.clone();
            let fetched = thread::spawn(move || fetch(&target)).join();
            fetched.expect("fetching binaries does not panic")?;
        }
        // What this fetched is the version it asked for, and every command comes through here.
        Ok(Self {
            bin: installed.join("bin"),
        })
    }

    /// The binaries in `bin`, if they are of the version every lake runs.
    pub fn at(bin: PathBuf) -> Result<Self> {
        let binaries = Self { bin };
        let found = binaries.major()?;
        if found != MAJOR {
            return Err(Error::Binaries(format!(
                "{} is Postgres {found}, and this metalake runs {MAJOR}",
                binaries.bin.display()
            )));
        }
        Ok(binaries)
    }

    pub(crate) fn tool(&self, name: &str) -> Command {
        Command::new(self.bin.join(name))
    }

    pub fn directory(&self) -> &Path {
        &self.bin
    }

    /// The major version of these binaries, from what `pg_ctl` says of itself.
    fn major(&self) -> Result<u32> {
        let output = self
            .tool("pg_ctl")
            .arg("--version")
            .output()
            .map_err(|e| Error::Binaries(format!("{}: {e}", self.bin.display())))?;
        let version = String::from_utf8_lossy(&output.stdout);
        let major = version
            .split_whitespace()
            .last()
            .and_then(|v| v.split('.').next())
            .and_then(|v| v.parse().ok());
        major.ok_or_else(|| Error::Binaries(format!("{}: pg_ctl says {version}", self.bin.display())))
    }
}

/// Fetches the binaries into `target`, which appears whole or not at all.
fn fetch(target: &Path) -> Result<()> {
    let wanted = VersionReq::parse(&format!("={MAJOR}")).expect("a version requirement");
    let (_, archive) = get_archive(URL, &wanted)?;
    extract(URL, &archive, target)?;
    Ok(())
}

fn home() -> Result<PathBuf> {
    env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .ok_or_else(|| Error::Binaries(format!("no home directory: set {ENVIRONMENT}")))
}
