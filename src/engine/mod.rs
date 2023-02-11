use sqlx::{Any, AnyPool, Database, Pool, Postgres, Sqlite};

mod database;
pub mod db_mapping;

pub struct Engine<T: Database> {
    pub pool: Box<Pool<T>>,
    pub database: Option<database::Database>,
}

impl Engine<Any> {
    pub fn new(pool: AnyPool) -> Self {
        Self {
            pool: Box::new(pool),
            database: None,
        }
    }

    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }
}

impl Engine<Postgres> {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool: Box::new(pool),
            database: None,
        }
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

impl Engine<Sqlite> {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self {
            pool: Box::new(pool),
            database: None,
        }
    }

    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{query, SqlitePool};

    use super::*;

    #[tokio::test]
    async fn test_engine_creation() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let engine = Engine::<Sqlite>::new(pool);
        assert!(query("SELECT 1").execute(engine.pool()).await.is_ok());
    }
}
