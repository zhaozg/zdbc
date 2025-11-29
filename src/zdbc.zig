//! ZDBC - Zig Database Connector
//!
//! A high-performance, type-safe database abstraction layer using VTable pattern.
//! Supports SQLite, PostgreSQL, and MySQL through a unified interface.

const std = @import("std");

pub const drivers = struct {
    pub const sqlite = @import("drivers/sqlite.zig");
    pub const postgresql = @import("drivers/postgresql.zig");
    pub const mysql = @import("drivers/mysql.zig");
};

pub const Connection = @import("connection.zig").Connection;
pub const Pool = @import("pool.zig").Pool;
pub const Value = @import("value.zig").Value;
pub const Result = @import("result.zig").Result;
pub const Row = @import("result.zig").Row;
pub const Statement = @import("statement.zig").Statement;
pub const Error = @import("error.zig").Error;
pub const Uri = @import("uri.zig").Uri;

/// Database backend type
pub const Backend = enum {
    sqlite,
    postgresql,
    mysql,
};

/// Create a new database connection from a URI string
/// Supported URI formats:
/// - sqlite:///path/to/database.db
/// - sqlite::memory:
/// - postgresql://user:password@host:port/database
/// - mysql://user:password@host:port/database
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
    };
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
}
