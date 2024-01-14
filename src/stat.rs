//! Statistics about the current state of the system.

use crate::schema::Constraint;

/// Statistics about the query result.
pub enum QueryStat {
    /// Number of rows in the result.
    Query(usize),
    /// Number of rows affected.
    Update(usize),
    /// Description of a table.
    Desc(Vec<Constraint>),
}
