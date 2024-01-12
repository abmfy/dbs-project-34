//! Statistics about the current state of the system.

/// Statistics about the query result.
pub enum QueryStat {
    /// Number of rows in the result.
    Query(usize),
    /// Number of rows affected.
    Update(usize),
}
