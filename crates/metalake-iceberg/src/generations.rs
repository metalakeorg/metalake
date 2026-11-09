//! The tables whose projection a load of this process is generating (REST-6), one load at a time each: loads of a table
//! that come together generate it once, as each that waited finds what the one before wrote, and generates only what is
//! still missing. Loads of different tables do not wait for each other. Other processes are not seen: they generate the
//! same files, as REST-6 allows.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as Turn, OwnedMutexGuard};
use uuid::Uuid;

/// Each table being generated, and the turn its loads wait for. An entry goes when its last load does.
#[derive(Debug, Clone, Default)]
pub(crate) struct Generations {
    tables: Arc<Mutex<HashMap<Uuid, Arc<Turn<()>>>>>,
}

/// A load's turn to generate `table`, held until it is dropped.
#[derive(Debug)]
pub(crate) struct Generating {
    table: Uuid,
    tables: Arc<Mutex<HashMap<Uuid, Arc<Turn<()>>>>>,
    _turn: OwnedMutexGuard<()>,
}

impl Generations {
    /// Waits for the turn of `table`: at once if no load of this process generates it, or when the one before is done.
    pub(crate) async fn turn(&self, table: Uuid) -> Generating {
        let turn = {
            let mut tables = self.tables.lock().expect("not poisoned");
            tables.entry(table).or_default().clone()
        };
        Generating {
            table,
            tables: self.tables.clone(),
            _turn: turn.lock_owned().await,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.tables.lock().expect("not poisoned").len()
    }
}

impl Drop for Generating {
    fn drop(&mut self) {
        let mut tables = self.tables.lock().expect("not poisoned");
        // The map holds one, and this turn another: any more is a load waiting, which keeps the entry.
        if tables.get(&self.table).is_some_and(|turn| Arc::strong_count(turn) == 2) {
            tables.remove(&self.table);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn rest6_concurrent_loads_one_table_serializes() {
        let generations = Generations::default();
        let table = Uuid::new_v4();
        let first = generations.turn(table).await;
        let waiting = tokio::spawn({
            let generations = generations.clone();
            async move { generations.turn(table).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished(), "a second load waits while the first generates");
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(generations.len(), 1, "the table stays while its second load holds it");
        drop(second);
        assert_eq!(generations.len(), 0, "the last load removes its table");
    }

    #[tokio::test]
    async fn rest6_loads_other_tables_isolates() {
        let generations = Generations::default();
        let _one = generations.turn(Uuid::new_v4()).await;
        let other = tokio::time::timeout(Duration::from_secs(1), generations.turn(Uuid::new_v4())).await;
        assert!(other.is_ok(), "a load of another table does not wait");
    }
}
