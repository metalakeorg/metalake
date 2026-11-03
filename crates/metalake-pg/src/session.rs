//! Where a call runs: a connection or a transaction, of `tokio-postgres` or of a `deadpool-postgres` pool, which
//! prepares each statement once per connection.

use std::future::Future;

use tokio_postgres::{GenericClient, Statement};

/// A connection or a transaction a call runs on. A pool's client caches its prepared statements; a plain
/// `tokio-postgres` client prepares them on each call.
pub trait Session: Sync {
    /// The statement for `sql`, prepared on this session's connection.
    fn statement(&self, sql: &str) -> impl Future<Output = Result<Statement, tokio_postgres::Error>> + Send;

    /// The client the statement runs on.
    fn client(&self) -> &impl GenericClient;
}

impl Session for tokio_postgres::Client {
    fn statement(&self, sql: &str) -> impl Future<Output = Result<Statement, tokio_postgres::Error>> + Send {
        self.prepare(sql)
    }

    fn client(&self) -> &impl GenericClient {
        self
    }
}

impl Session for tokio_postgres::Transaction<'_> {
    fn statement(&self, sql: &str) -> impl Future<Output = Result<Statement, tokio_postgres::Error>> + Send {
        self.prepare(sql)
    }

    fn client(&self) -> &impl GenericClient {
        self
    }
}

impl Session for deadpool_postgres::Client {
    fn statement(&self, sql: &str) -> impl Future<Output = Result<Statement, tokio_postgres::Error>> + Send {
        self.prepare_cached(sql)
    }

    fn client(&self) -> &impl GenericClient {
        &***self
    }
}

impl Session for deadpool_postgres::Transaction<'_> {
    fn statement(&self, sql: &str) -> impl Future<Output = Result<Statement, tokio_postgres::Error>> + Send {
        self.prepare_cached(sql)
    }

    fn client(&self) -> &impl GenericClient {
        &**self
    }
}
