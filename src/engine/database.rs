use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
};

use super::{db_mapping::DbGraph, Engine};

#[derive(Debug, Clone)]
pub struct ColumnDependency {
    pub table_name: String,
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: String,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub is_nullable: String,
    pub dependent_on: Option<ColumnDependency>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Database {
    pub tables: Vec<Table>,
}

impl Database {
    pub fn from_graph(graph: DbGraph) -> Self {
        let mut tables = Vec::new();
        let mut table_names = HashSet::new();

        for (table, _) in graph.iter() {
            table_names.insert(table.table.clone());
        }

        for table_name in table_names {
            let mut columns = Vec::new();

            for (col_key, col_value) in graph.iter() {
                if col_key.table.clone() == table_name {
                    let mut dependent_on = None;

                    if let Some(dependent_on_col_key) = &col_value.dependent_on {
                        dependent_on = Some(ColumnDependency {
                            table_name: dependent_on_col_key.table.clone(),
                            column_name: dependent_on_col_key.column.clone(),
                            data_type: col_value.data_type.clone(),
                            is_nullable: col_value.is_nullable.clone(),
                        });
                    }

                    columns.push(Column {
                        name: col_key.column.clone(),
                        data_type: col_value.data_type.clone(),
                        is_nullable: col_value.is_nullable.clone(),
                        dependent_on,
                    });
                }
            }

            tables.push(Table {
                name: table_name.clone(),
                columns,
            });
        }

        Database { tables }
    }
}

impl Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for table in self.tables.iter() {
            writeln!(f, "= {}", table.name)?;

            for column in table.columns.iter() {
                write!(
                    f,
                    " -> {} |{}| nullable: {}",
                    column.name, column.data_type, column.is_nullable
                )?;

                if let Some(dependent_on) = &column.dependent_on {
                    write!(
                        f,
                        " (dependent on {}.{} |{}| nullable: {})",
                        dependent_on.table_name,
                        dependent_on.column_name,
                        dependent_on.data_type,
                        dependent_on.is_nullable
                    )?;
                }

                writeln!(f)?;
            }
        }

        Ok(())
    }
}

impl Database {
    pub fn get_table(&self, table_name: &str) -> Option<&Table> {
        self.tables.iter().find(|table| table.name == table_name)
    }

    pub fn get_column(&self, table_name: &str, column_name: &str) -> Option<&Column> {
        self.get_table(table_name).and_then(|table| {
            table
                .columns
                .iter()
                .find(|column| column.name == column_name)
        })
    }

    pub fn get_table_dependencies(&self, table_name: &str) -> Vec<&Table> {
        let mut dependencies = Vec::new();

        if let Some(table) = self.get_table(table_name) {
            for column in table.columns.iter() {
                if let Some(dependent_on) = &column.dependent_on {
                    if let Some(dependent_on_table) = self.get_table(&dependent_on.table_name) {
                        dependencies.push(dependent_on_table);
                    }
                }
            }
        }

        dependencies
    }

    pub fn get_full_table_dependencies(&self, table_name: &str) -> Vec<&Table> {
        let mut dependencies = Vec::new();

        if let Some(table) = self.get_table(table_name) {
            for column in table.columns.iter() {
                if let Some(dependent_on) = &column.dependent_on {
                    if let Some(dependent_on_table) = self.get_table(&dependent_on.table_name) {
                        dependencies.push(dependent_on_table);

                        let mut dependent_on_table_dependencies =
                            self.get_full_table_dependencies(&dependent_on.table_name);

                        dependencies.append(&mut dependent_on_table_dependencies);
                    }
                }
            }
        }

        dependencies.reverse();
        dependencies
    }
}

// TODO: Figure out how to make this work without creating a new database
pub fn build_database<T>(engine: Engine<T>, database: Database) -> Engine<T>
where
    T: sqlx::Database,
{
    Engine {
        database: Some(database),
        pool: engine.pool,
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{postgres::PgPoolOptions, Postgres};

    use super::*;
    use crate::engine::{database::Database, Engine};

    #[tokio::test]
    async fn test_build_database() {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:postgres@localhost:5432/postgres")
            .await
            .unwrap();
        let engine = Engine::<Postgres>::new(pool);
        let graph = engine.build_dependency_graph().await;

        let database = Database::from_graph(graph);

        let new_engine = build_database(engine, database);
        assert!(new_engine.database.is_some());
    }
}
