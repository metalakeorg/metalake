//! The connections a client of the protocol holds: a pool, whose clients prepare each statement once. It lives in
//! the SDK so that no other crate depends on a Postgres driver (ADR 0003 §3.5).

use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod};
use tokio_postgres::NoTls;

use crate::{Error, ErrorKind, Result};

/// A pooled connection: a [`Session`](crate::Session) that prepares each statement once.
pub type PooledSession = deadpool_postgres::Client;

/// A pool of connections to one Postgres database.
#[derive(Clone)]
pub struct Pool(deadpool_postgres::Pool);

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").field("status", &self.0.status()).finish()
    }
}

impl Pool {
    /// A pool of up to `size` connections to the database of `url`, a libpq connection string or URL. No connection
    /// is opened until one is asked for.
    pub fn new(url: &str, size: usize) -> Result<Self> {
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
            .build()
            .map_err(|e| Error::new(ErrorKind::Postgres, e.to_string()))?;
        Ok(Self(pool))
    }

    /// A connection of the pool, opened if none is free.
    pub async fn get(&self) -> Result<PooledSession> {
        self.0.get().await.map_err(|e| match e {
            deadpool_postgres::PoolError::Backend(e) => Error::from(e),
            e => Error::new(ErrorKind::Unknown, e.to_string()),
        })
    }
}
