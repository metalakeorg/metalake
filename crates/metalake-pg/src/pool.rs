//! The connections a client of the protocol holds: a pool, whose clients prepare each statement once. It lives in
//! the SDK so that no other crate depends on a Postgres driver (ADR 0003 §3.5).

use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use crate::{Error, ErrorKind, Result};

/// How long a caller waits for a connection when all are taken.
const WAIT: Duration = Duration::from_secs(60);

/// A pooled connection: a [`Session`](crate::Session) that prepares each statement once.
pub type PooledSession = deadpool_postgres::Client;

/// A pool of connections to one Postgres database.
#[derive(Clone)]
pub struct Pool(deadpool_postgres::Pool, Duration);

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        let config: tokio_postgres::Config = url
            .parse()
            .map_err(|e: tokio_postgres::Error| Error::new(ErrorKind::Postgres, e.to_string()))?;
        let manager = Manager::from_config(
            config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = deadpool_postgres::Pool::builder(manager)
            .max_size(size)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(wait))
            .build()
            .map_err(|e| Error::new(ErrorKind::Postgres, e.to_string()))?;
        Ok(Self(pool, wait))
    }

    /// Closes the idle connections, and those in use as they come back. The pool opens no more.
    pub fn close(&self) {
        self.0.close();
    }

    /// A connection of the pool, opened if none is free.
    pub async fn get(&self) -> Result<PooledSession> {
        self.0.get().await.map_err(|e| match e {
            deadpool_postgres::PoolError::Backend(e) => Error::from(e),
            deadpool_postgres::PoolError::Timeout(_) => {
                let (wait, size) = (self.1.as_secs_f64(), self.0.status().max_size);
                Error::new(
                    ErrorKind::Postgres,
                    format!("no connection of {size} came free in {wait}s"),
                )
            }
            e => Error::new(ErrorKind::Unknown, e.to_string()),
        })
    }
}
