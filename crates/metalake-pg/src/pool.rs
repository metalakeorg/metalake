//! The connections a client of the protocol holds: a pool, whose clients prepare each statement once. It lives in
//! the SDK so that no other crate depends on a Postgres driver (ADR 0003 §3.5).
//!
//! A pool connects as its connection string says, over TLS as [`tls`] reads it. Given a [`Credential`], it connects
//! with the password the credential answers instead, which may last minutes, as a cloud's token does: Postgres checks
//! a password when a connection opens and never again, so a connection outlives the password it opened with, and a
//! new one is opened with a fresh one.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::{Connect, Manager, ManagerConfig, RecyclingMethod, Runtime};
use futures_util::future::BoxFuture;
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::{Error, ErrorKind, Result, tls};

/// How long a caller waits for a connection when all are taken.
const WAIT: Duration = Duration::from_secs(60);
/// How long before a password expires a new one is asked for, so that none expires while a connection opens with it.
const MARGIN: Duration = Duration::from_secs(60);

/// A pooled connection: a [`Session`](crate::Session) that prepares each statement once.
pub type PooledSession = deadpool_postgres::Client;

/// A password, and when it expires if it does.
#[derive(Clone)]
pub struct Password {
    pub value: String,
    pub expires: Option<Instant>,
}

impl fmt::Debug for Password {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Password")
            .field("expires", &self.expires)
            .finish_non_exhaustive()
    }
}

/// What answers the password a connection opens with: a cloud's token for its identity, minted when one is asked for.
pub trait Credential: Send + Sync + fmt::Debug {
    fn password(&self) -> BoxFuture<'_, Result<Password>>;
}

/// A credential's password, kept until shortly before it expires.
#[derive(Debug)]
struct Fresh {
    credential: Arc<dyn Credential>,
    current: Mutex<Option<Password>>,
}

impl Fresh {
    /// The password, asked for again when there is none or it is about to expire.
    async fn password(&self) -> Result<String> {
        let mut current = self.current.lock().await;
        let stale = match &*current {
            None => true,
            Some(p) => p
                .expires
                .is_some_and(|at| at.saturating_duration_since(Instant::now()) < MARGIN),
        };
        if stale {
            *current = Some(self.credential.password().await?);
        }
        Ok(current.as_ref().map(|p| p.value.clone()).unwrap_or_default())
    }
}

/// Opens a connection with the credential's password of the moment.
struct WithCredential {
    tls: MakeRustlsConnect,
    fresh: Arc<Fresh>,
}

impl Connect for WithCredential {
    fn connect(
        &self,
        config: &tokio_postgres::Config,
    ) -> BoxFuture<'_, Result<(Client, tokio::task::JoinHandle<()>), tokio_postgres::Error>> {
        let (tls, mut config) = (self.tls.clone(), config.clone());
        Box::pin(async move {
            // The pool asked for one before it asked for a connection (`Pool::get`), so this answers it.
            let password = self
                .fresh
                .current
                .lock()
                .await
                .as_ref()
                .map(|p| p.value.clone())
                .unwrap_or_default();
            config.password(password);
            let (client, connection) = config.connect(tls).await?;
            Ok((client, tokio::spawn(async move { drop(connection.await) })))
        })
    }
}

/// Who connects where, as a connection string says: what a cloud's token is minted for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The first host, by name or address.
    pub host: String,
    pub port: u16,
    pub user: String,
}

impl Target {
    /// The target of a libpq connection string or URL.
    pub fn of(url: &str) -> Result<Self> {
        let (rest, tls) = tls::split(url)?;
        let config: tokio_postgres::Config = tls
            .driver_string(&rest)
            .parse()
            .map_err(|e: tokio_postgres::Error| Error::new(ErrorKind::Postgres, e.to_string()))?;
        // A socket is a host only on Unix, so that elsewhere every host is one by name or address.
        #[allow(clippy::unnecessary_find_map)]
        let host = config.get_hosts().iter().find_map(|host| match host {
            tokio_postgres::config::Host::Tcp(name) => Some(name.clone()),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(_) => None,
        });
        let host = host.ok_or_else(|| Error::new(ErrorKind::Postgres, "the connection string names no host"))?;
        let user = config
            .get_user()
            .ok_or_else(|| Error::new(ErrorKind::Postgres, "the connection string names no user"))?
            .to_string();
        let port = config.get_ports().first().copied().unwrap_or(5432);
        Ok(Self { host, port, user })
    }
}

/// A pool of connections to one Postgres database.
#[derive(Clone)]
pub struct Pool(deadpool_postgres::Pool, Duration, Option<Arc<Fresh>>);

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool").field("status", &self.0.status()).finish()
    }
}

impl Pool {
    /// A pool of up to `size` connections to the database of `url`, a libpq connection string or URL. No connection
    /// is opened until one is asked for.
    pub fn new(url: &str, size: usize) -> Result<Self> {
        Self::waiting(url, size, WAIT)
    }

    /// The same, with callers waiting `wait` for a connection, and then refused: nothing was sent, so it is a plain failure.
    pub fn waiting(url: &str, size: usize, wait: Duration) -> Result<Self> {
        Self::connecting(url, size, wait, None)
    }

    /// The same, connecting with `credential`'s password, when one is given, in place of any the string names.
    pub fn connecting(url: &str, size: usize, wait: Duration, credential: Option<Arc<dyn Credential>>) -> Result<Self> {
        let (rest, tls) = tls::split(url)?;
        let config: tokio_postgres::Config = tls
            .driver_string(&rest)
            .parse()
            .map_err(|e: tokio_postgres::Error| Error::new(ErrorKind::Postgres, e.to_string()))?;
        let settings = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let fresh = credential.map(|credential| {
            Arc::new(Fresh {
                credential,
                current: Mutex::new(None),
            })
        });
        let manager = match &fresh {
            None => Manager::from_config(config, tls.connector()?, settings),
            Some(fresh) => {
                let connect = WithCredential {
                    tls: tls.connector()?,
                    fresh: fresh.clone(),
                };
                Manager::from_connect(config, connect, settings)
            }
        };
        let pool = deadpool_postgres::Pool::builder(manager)
            .max_size(size)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(wait))
            .build()
            .map_err(|e| Error::new(ErrorKind::Postgres, e.to_string()))?;
        Ok(Self(pool, wait, fresh))
    }

    /// Closes the idle connections, and those in use as they come back. The pool opens no more.
    pub fn close(&self) {
        self.0.close();
    }

    /// A connection of the pool, opened if none is free.
    /// With a credential, its password is made fresh first, for a connection this may open. When the credential fails,
    /// an open connection is still given, as it signed in already; only one that must open says why it cannot.
    pub async fn get(&self) -> Result<PooledSession> {
        let unrefreshed = match &self.2 {
            Some(fresh) => fresh.password().await.err(),
            None => None,
        };
        self.0.get().await.map_err(|e| match (e, unrefreshed) {
            (deadpool_postgres::PoolError::Backend(_), Some(credential)) => {
                credential.context("the catalog's credential")
            }
            (e, _) => self.failure(e),
        })
    }

    fn failure(&self, e: deadpool_postgres::PoolError) -> Error {
        match e {
            deadpool_postgres::PoolError::Backend(e) => Error::from(e),
            deadpool_postgres::PoolError::Timeout(_) => {
                let (wait, size) = (self.1.as_secs_f64(), self.0.status().max_size);
                Error::new(
                    ErrorKind::Postgres,
                    format!("no connection of {size} came free in {wait}s"),
                )
            }
            e => Error::new(ErrorKind::Unknown, e.to_string()),
        }
    }
}
