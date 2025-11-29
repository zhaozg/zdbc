//! ZDBC - Zig Database Connector
//!
//! A high-performance, type-safe database abstraction layer using VTable pattern.
//! Supports SQLite, PostgreSQL, and MySQL through a unified interface.
//!
//! ## Features
//! - VTable-based polymorphism for zero-cost abstraction
//! - Unified interface for multiple database backends
//! - Connection pooling support
//! - Prepared statement support
//! - Transaction support
//! - URI-based connection strings
//!
//! ## Usage
//! ```zig
//! const zdbc = @import("zdbc");
//!
//! // Open a connection
//! var conn = try zdbc.open(allocator, "sqlite:///path/to/db.sqlite");
//! defer conn.close();
//!
//! // Execute a query
//! _ = try conn.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", &.{});
//!
//! // Insert with parameters
//! _ = try conn.exec("INSERT INTO users (name) VALUES (?)", &.{zdbc.Value.initText("John")});
//!
//! // Query rows
//! var result = try conn.query("SELECT * FROM users", &.{});
//! defer result.deinit();
//!
//! while (try result.next()) |row| {
//!     const id = (try row.getInt(0)).?;
//!     const name = (try row.getText(1)).?;
//!     std.debug.print("User: {} - {s}\n", .{id, name});
//! }
//! ```

const std = @import("std");

/// Database drivers
pub const drivers = struct {
    pub const sqlite = @import("drivers/sqlite.zig");
    pub const postgresql = @import("drivers/postgresql.zig");
    pub const mysql = @import("drivers/mysql.zig");
    pub const mock = @import("drivers/mock.zig");
};

// Core types
pub const Connection = @import("connection.zig").Connection;
pub const ConnectionVTable = @import("connection.zig").ConnectionVTable;
pub const Pool = @import("pool.zig").Pool;
pub const PoolConfig = @import("pool.zig").PoolConfig;
pub const PooledConnection = @import("pool.zig").PooledConnection;

// Value and result types
pub const Value = @import("value.zig").Value;
pub const fromAny = @import("value.zig").fromAny;
pub const Result = @import("result.zig").Result;
pub const ResultVTable = @import("result.zig").ResultVTable;
pub const Row = @import("result.zig").Row;
pub const Statement = @import("statement.zig").Statement;
pub const StatementVTable = @import("statement.zig").StatementVTable;

// Error and URI types
pub const Error = @import("error.zig").Error;
pub const Uri = @import("uri.zig").Uri;

/// Database backend type
pub const Backend = enum {
    sqlite,
    postgresql,
    mysql,
    mock, // For testing purposes
};

/// Create a new database connection from a URI string
/// Supported URI formats:
/// - sqlite:///path/to/database.db
/// - sqlite://:memory:  (in-memory database)
/// - postgresql://user:password@host:port/database
/// - mysql://user:password@host:port/database
///
/// SQL injection prevention: Always use parameterized queries
/// instead of string concatenation.
pub fn open(allocator: std.mem.Allocator, uri_string: []const u8) Error!Connection {
    const uri = Uri.parse(uri_string) catch return Error.InvalidUri;
    return openWithUri(allocator, uri);
}

/// Create a new database connection from a parsed URI
pub fn openWithUri(allocator: std.mem.Allocator, uri: Uri) Error!Connection {
    return switch (uri.backend) {
        .sqlite => drivers.sqlite.open(allocator, uri),
        .postgresql => drivers.postgresql.open(allocator, uri),
        .mysql => drivers.mysql.open(allocator, uri),
        .mock => drivers.mock.open(allocator, uri),
    };
}

/// Open a mock connection for testing
pub fn openMock(allocator: std.mem.Allocator) Error!Connection {
    const uri = Uri.parse("mock://localhost/test") catch return Error.InvalidUri;
    return drivers.mock.open(allocator, uri);
}

test {
    _ = @import("connection.zig");
    _ = @import("value.zig");
    _ = @import("result.zig");
    _ = @import("statement.zig");
    _ = @import("error.zig");
    _ = @import("uri.zig");
    _ = @import("pool.zig");
    _ = drivers.sqlite;
    _ = drivers.postgresql;
    _ = drivers.mysql;
    _ = drivers.mock;
}

test "open mock connection" {
    var conn = try openMock(std.testing.allocator);
    defer conn.close();

    try conn.ping();
}

test "open with invalid URI" {
    const result = open(std.testing.allocator, "invalid://uri");
    try std.testing.expectError(Error.InvalidUri, result);
}
