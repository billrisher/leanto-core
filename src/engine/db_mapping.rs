use std::collections::HashMap;

use sqlx::{FromRow, PgPool, Pool, Postgres};

use super::Engine;

pub type DbGraph = HashMap<ColKey, ColValue>;

// TODO: Structure better
// TODO: Clean up code

#[derive(Debug, Clone, FromRow)]
pub struct TableResult {
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub udt_name: Option<String>,
    pub is_nullable: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ColKey {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone)]
pub struct ColValue {
    pub data_type: String,
    pub is_nullable: String,
    pub dependent_on: Option<ColKey>,
}

#[derive(Debug, Clone, FromRow)]
pub struct TableDependency {
    pub table_schema: Option<String>,
    pub constraint_name: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub foreign_table_schema: Option<String>,
    pub foreign_table_name: Option<String>,
    pub foreign_column_name: Option<String>,
    pub data_type: Option<String>,
    pub is_nullable: Option<String>,
    pub foreign_data_type: Option<String>,
    pub foreign_is_nullable: Option<String>,
}
impl Engine<Postgres> {
    async fn get_table_data(&self) -> Result<Vec<TableResult>, sqlx::Error> {
        dotenvy::dotenv().ok();
        let res: Vec<TableResult> = sqlx::query_as::<_, TableResult>(
            "SELECT
  information_schema.columns.table_name,
  information_schema.columns.column_name,
  information_schema.columns.udt_name, information_schema.columns.is_nullable
from information_schema.tables
inner join information_schema.columns
on information_schema.tables.table_name = information_schema.columns.table_name
  where information_schema.tables.table_schema = 'public'
order by information_schema.columns.table_name",
        )
        .fetch_all(self.pool())
        .await
        .unwrap();
        Ok(res)
    }

    async fn get_table_dependencies(&self) -> Result<Vec<TableDependency>, sqlx::Error> {
        sqlx::query_as::<_, TableDependency>(
            "SELECT DISTINCT
    tc.table_schema,
    tc.constraint_name,
    tc.table_name,
    kcu.column_name,
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name,
    native_cols.udt_name as data_type,
    native_cols.is_nullable as is_nullable,
    foreign_cols.udt_name as foreign_data_type,
    foreign_cols.is_nullable as foreign_is_nullable
FROM
    information_schema.table_constraints AS tc
    INNER JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    INNER JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    INNER JOIN information_schema.columns AS native_cols
         ON native_cols.column_name = kcu.column_name
         AND native_cols.table_name = kcu.table_name
    INNER JOIN information_schema.columns AS foreign_cols
         ON ccu.table_name = foreign_cols.table_name
         AND ccu.column_name = foreign_cols.column_name
WHERE tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name",
        )
        .fetch_all(self.pool())
        .await
    }

    pub async fn build_dependency_graph(&self) -> DbGraph {
        let table_data = self.get_table_data().await.unwrap();
        let dependencies = self.get_table_dependencies().await.unwrap();

        let mut hash = DbGraph::new();

        for row in table_data {
            hash.insert(
                ColKey {
                    table: row.table_name.unwrap(),
                    column: row.column_name.unwrap(),
                },
                ColValue {
                    data_type: row.udt_name.unwrap(),
                    is_nullable: row.is_nullable.unwrap(),
                    dependent_on: None,
                },
            );
        }

        for dependency in dependencies {
            hash.entry(ColKey {
                table: dependency.table_name.clone().unwrap(),
                column: dependency.column_name.clone().unwrap(),
            })
            .and_modify(|v| {
                v.dependent_on = Some(ColKey {
                    table: dependency.foreign_table_name.unwrap(),
                    column: dependency.foreign_column_name.unwrap(),
                });
            });
        }

        hash
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{postgres::PgPoolOptions, Postgres};

    use crate::engine::Engine;

    use super::*;

    #[tokio::test]
    async fn test_get_table_data() {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:postgres@localhost:5432/postgres")
            .await
            .unwrap();
        let engine = Engine::<Postgres>::new(pool);
        let res = engine.get_table_data().await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_get_table_results() {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:postgres@localhost:5432/postgres")
            .await
            .unwrap();
        let engine = Engine::<Postgres>::new(pool);
        assert!(engine.get_table_dependencies().await.is_ok());
    }

    #[tokio::test]
    async fn test_build_dependency_graph() {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:postgres@localhost:5432/postgres")
            .await
            .unwrap();
        let engine = Engine::<Postgres>::new(pool);
        let graph = engine.build_dependency_graph().await;
        assert!(graph.len() > 0);
    }
}
