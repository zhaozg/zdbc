//! Connection interface using VTable pattern for database abstraction
//!
//! This module provides a unified interface for database operations
//! using Zig's VTable pattern for zero-cost abstraction.

const std = @import("std");
const Value = @import("value.zig").Value;
const Result = @import("result.zig").Result;
const Row = @import("result.zig").Row;
const Statement = @import("statement.zig").Statement;
const Error = @import("error.zig").Error;
const Uri = @import("uri.zig").Uri;

/// VTable for database connection operations
/// This is the core interface that all database drivers must implement
pub const ConnectionVTable = struct {
    /// Execute a query that does not return rows (INSERT, UPDATE, DELETE, CREATE, etc.)
    /// Returns the number of affected rows
    exec: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8, params: []const Value) Error!usize,

    /// Execute a query that returns rows (SELECT)
    query: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8, params: []const Value) Error!Result,

    /// Prepare a statement for repeated execution
    prepare: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8) Error!Statement,

    /// Begin a transaction
    begin: *const fn (ctx: *anyopaque) Error!void,

    /// Commit the current transaction
    commit: *const fn (ctx: *anyopaque) Error!void,

    /// Rollback the current transaction
    rollback: *const fn (ctx: *anyopaque) Error!void,

    /// Close the connection and release resources
    close: *const fn (ctx: *anyopaque) void,

    /// Get the last insert row ID (if applicable)
    lastInsertId: *const fn (ctx: *anyopaque) ?i64,

    /// Get the number of rows affected by the last operation
    affectedRows: *const fn (ctx: *anyopaque) usize,

    /// Ping the database to check connection health
    ping: ?*const fn (ctx: *anyopaque) Error!void,

    /// Get a human-readable error message for the last error
    lastError: ?*const fn (ctx: *anyopaque) ?[]const u8,
};

/// Database connection with VTable-based polymorphism
/// Provides a unified interface for all supported databases
pub const Connection = struct {
    /// Pointer to the driver-specific implementation
    ctx: *anyopaque,

    /// VTable containing function pointers
    vtable: *const ConnectionVTable,

    /// Allocator used for this connection
    allocator: std.mem.Allocator,

    /// The URI used to create this connection
    uri: Uri,

    /// Execute a query that does not return rows
    /// SQL injection prevention: Use parameterized queries with params
    pub fn exec(self: *const Connection, sql: []const u8, params: []const Value) Error!usize {
        return self.vtable.exec(self.ctx, self.allocator, sql, params);
    }

    /// Execute a query with no parameters
    pub fn execNoParams(self: *const Connection, sql: []const u8) Error!usize {
        return self.exec(sql, &.{});
    }

    /// Execute a query that returns rows
    /// SQL injection prevention: Use parameterized queries with params
    pub fn query(self: *const Connection, sql: []const u8, params: []const Value) Error!Result {
        return self.vtable.query(self.ctx, self.allocator, sql, params);
    }

    /// Execute a query with no parameters
    pub fn queryNoParams(self: *const Connection, sql: []const u8) Error!Result {
        return self.query(sql, &.{});
    }

    /// Get a single row from a query, or null if no rows
    pub fn row(self: *const Connection, sql: []const u8, params: []const Value) Error!?Row {
        var result = try self.query(sql, params);
        defer result.deinit();

        return result.next();
    }

    /// Prepare a statement for repeated execution
    pub fn prepare(self: *const Connection, sql: []const u8) Error!Statement {
        return self.vtable.prepare(self.ctx, self.allocator, sql);
    }

    /// Begin a transaction
    pub fn begin(self: *const Connection) Error!void {
        return self.vtable.begin(self.ctx);
    }

    /// Commit the current transaction
    pub fn commit(self: *const Connection) Error!void {
        return self.vtable.commit(self.ctx);
    }

    /// Rollback the current transaction
    pub fn rollback(self: *const Connection) Error!void {
        return self.vtable.rollback(self.ctx);
    }

    /// Execute a block within a transaction
    /// Automatically commits on success, rollbacks on error
    pub fn transaction(self: *const Connection, comptime func: fn (*const Connection) Error!void) Error!void {
        try self.begin();
        errdefer self.rollback() catch {};

        try func(self);
        try self.commit();
    }

    /// Close the connection
    pub fn close(self: *Connection) void {
        self.vtable.close(self.ctx);
    }

    /// Alias for close
    pub fn deinit(self: *Connection) void {
        self.close();
    }

    /// Get the last insert row ID
    pub fn lastInsertId(self: *const Connection) ?i64 {
        return self.vtable.lastInsertId(self.ctx);
    }

    /// Get the number of rows affected by the last operation
    pub fn affectedRows(self: *const Connection) usize {
        return self.vtable.affectedRows(self.ctx);
    }

    /// Ping the database to check connection health
    pub fn ping(self: *const Connection) Error!void {
        if (self.vtable.ping) |ping_fn| {
            return ping_fn(self.ctx);
        }
        return Error.NotImplemented;
    }

    /// Get the last error message
    pub fn lastError(self: *const Connection) ?[]const u8 {
        if (self.vtable.lastError) |err_fn| {
            return err_fn(self.ctx);
        }
        return null;
    }
};

test "Connection VTable structure" {
    // Verify VTable has correct size and alignment
    try std.testing.expect(@sizeOf(ConnectionVTable) > 0);
    try std.testing.expect(@alignOf(ConnectionVTable) >= @alignOf(*anyopaque));
}
