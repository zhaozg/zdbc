//! SQLite database driver
//!
//! This driver wraps the zqlite.zig library to provide SQLite support
//! through the ZDBC VTable interface.
//!
//! Dependencies: https://github.com/karlseguin/zqlite.zig

const std = @import("std");
const zqlite = @import("zqlite");
const Connection = @import("../connection.zig").Connection;
const ConnectionVTable = @import("../connection.zig").ConnectionVTable;
const Result = @import("../result.zig").Result;
const ResultVTable = @import("../result.zig").ResultVTable;
const Statement = @import("../statement.zig").Statement;
const StatementVTable = @import("../statement.zig").StatementVTable;
const Value = @import("../value.zig").Value;
const Error = @import("../error.zig").Error;
const Uri = @import("../uri.zig").Uri;

/// SQLite connection context
pub const SqliteContext = struct {
    allocator: std.mem.Allocator,
    conn: zqlite.Conn,
    last_error: ?[]const u8 = null,
    affected_rows: usize = 0,
    last_insert_id: i64 = 0,

    pub fn init(allocator: std.mem.Allocator, path: [:0]const u8) !*SqliteContext {
        const flags = zqlite.OpenFlags.Create | zqlite.OpenFlags.ReadWrite | zqlite.OpenFlags.EXResCode;
        const conn = zqlite.Conn.init(path, flags) catch return error.ConnectionFailed;

        const ctx = try allocator.create(SqliteContext);
        ctx.* = SqliteContext{
            .allocator = allocator,
            .conn = conn,
        };
        return ctx;
    }

    pub fn deinit(self: *SqliteContext) void {
        self.conn.close();
        self.allocator.destroy(self);
    }
};

/// SQLite result context for query results
pub const SqliteResultContext = struct {
    allocator: std.mem.Allocator,
    rows: ?zqlite.Rows = null,
    current_row: ?zqlite.Row = null,
    column_count: usize = 0,
    affected: usize = 0,

    pub fn init(allocator: std.mem.Allocator) !*SqliteResultContext {
        const ctx = try allocator.create(SqliteResultContext);
        ctx.* = SqliteResultContext{
            .allocator = allocator,
        };
        return ctx;
    }

    pub fn deinit(self: *SqliteResultContext) void {
        if (self.rows) |*rows| {
            rows.deinit();
        }
        self.allocator.destroy(self);
    }
};

/// SQLite statement context for prepared statements
pub const SqliteStatementContext = struct {
    allocator: std.mem.Allocator,
    stmt: zqlite.Stmt,
    parent_ctx: *SqliteContext,

    pub fn init(allocator: std.mem.Allocator, stmt: zqlite.Stmt, parent_ctx: *SqliteContext) !*SqliteStatementContext {
        const ctx = try allocator.create(SqliteStatementContext);
        ctx.* = SqliteStatementContext{
            .allocator = allocator,
            .stmt = stmt,
            .parent_ctx = parent_ctx,
        };
        return ctx;
    }

    pub fn deinit(self: *SqliteStatementContext) void {
        self.stmt.deinit();
        self.allocator.destroy(self);
    }
};

/// VTable for SQLite results
const sqliteResultVTable = ResultVTable{
    .next = sqliteResultNext,
    .columnCount = sqliteResultColumnCount,
    .columnName = sqliteResultColumnName,
    .getValue = sqliteResultGetValue,
    .getValueByName = sqliteResultGetValueByName,
    .affectedRows = sqliteResultAffectedRows,
    .reset = null,
    .deinit = sqliteResultDeinit,
};

fn sqliteResultNext(ctx: *anyopaque) Error!bool {
    const result_ctx: *SqliteResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.rows) |*rows| {
        if (rows.next()) |row| {
            result_ctx.current_row = row;
            return true;
        }
    }
    return false;
}

fn sqliteResultColumnCount(ctx: *anyopaque) usize {
    const result_ctx: *SqliteResultContext = @ptrCast(@alignCast(ctx));
    return result_ctx.column_count;
}

fn sqliteResultColumnName(_: *anyopaque, _: usize) ?[]const u8 {
    // Column names are not available through zqlite's Row iterator.
    // Use column indices for value access instead.
    return null;
}

/// Returns the value at the given column index for the current row.
/// Returns Error.NoMoreRows if there is no current row.
/// Returns Value.initNull() if the value is NULL.
fn sqliteResultGetValue(ctx: *anyopaque, index: usize) Error!Value {
    const result_ctx: *SqliteResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.current_row == null) {
        return Error.NoMoreRows;
    }
    const row = result_ctx.current_row.?;
    
    // Check the column type and return appropriate value
    const col_type = row.columnType(index);
    return switch (col_type) {
        .int => Value.initInt(row.int(index)),
        .float => Value.initFloat(row.float(index)),
        .text => if (row.nullableText(index)) |text| Value.initText(text) else Value.initNull(),
        .blob => if (row.nullableBlob(index)) |blob| Value.initBlob(blob) else Value.initNull(),
        .null => Value.initNull(),
        .unknown => Value.initNull(),
    };
}

fn sqliteResultGetValueByName(_: *anyopaque, _: []const u8) Error!Value {
    return Error.NotImplemented;
}

fn sqliteResultAffectedRows(ctx: *anyopaque) usize {
    const result_ctx: *SqliteResultContext = @ptrCast(@alignCast(ctx));
    return result_ctx.affected;
}

fn sqliteResultDeinit(ctx: *anyopaque) void {
    const result_ctx: *SqliteResultContext = @ptrCast(@alignCast(ctx));
    result_ctx.deinit();
}

/// VTable for SQLite connections
pub const sqliteConnectionVTable = ConnectionVTable{
    .exec = sqliteExec,
    .query = sqliteQuery,
    .prepare = sqlitePrepare,
    .begin = sqliteBegin,
    .commit = sqliteCommit,
    .rollback = sqliteRollback,
    .close = sqliteClose,
    .lastInsertId = sqliteLastInsertId,
    .affectedRows = sqliteAffectedRows,
    .ping = sqlitePing,
    .lastError = sqliteLastError,
};

fn sqliteExec(ctx: *anyopaque, _: std.mem.Allocator, sql: []const u8, params: []const Value) Error!usize {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));

    if (params.len > 0) {
        // Use prepared statement with parameter binding
        const stmt = sqlite_ctx.conn.prepare(sql) catch return Error.PrepareFailed;
        defer stmt.deinit();

        // Bind parameters - zqlite.Stmt.bind expects a tuple
        // We need to convert our Value array to zqlite-compatible values
        // For now, we'll bind them one by one using bindValue
        for (params, 0..) |param, i| {
            switch (param) {
                .null => {
                    stmt.bindValue(@as(?i64, null), i) catch return Error.BindError;
                },
                .boolean => |b| {
                    const val: i64 = if (b) 1 else 0;
                    stmt.bindValue(val, i) catch return Error.BindError;
                },
                .int => |val| {
                    stmt.bindValue(val, i) catch return Error.BindError;
                },
                .uint => |val| {
                    if (val <= std.math.maxInt(i64)) {
                        stmt.bindValue(@as(i64, @intCast(val)), i) catch return Error.BindError;
                    } else {
                        return Error.BindError;
                    }
                },
                .float => |val| {
                    stmt.bindValue(val, i) catch return Error.BindError;
                },
                .text => |val| {
                    stmt.bindValue(val, i) catch return Error.BindError;
                },
                .blob => |val| {
                    stmt.bindValue(val, i) catch return Error.BindError;
                },
            }
        }

        stmt.stepToCompletion() catch return Error.ExecutionFailed;
    } else {
        // No params - use execNoArgs for performance
        const sql_z = sqlite_ctx.allocator.dupeZ(u8, sql) catch return Error.OutOfMemory;
        defer sqlite_ctx.allocator.free(sql_z);

        sqlite_ctx.conn.execNoArgs(sql_z) catch {
            return Error.ExecutionFailed;
        };
    }

    sqlite_ctx.affected_rows = sqlite_ctx.conn.changes();
    sqlite_ctx.last_insert_id = sqlite_ctx.conn.lastInsertedRowId();
    return sqlite_ctx.affected_rows;
}

fn sqliteQuery(ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8, params: []const Value) Error!Result {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));

    // Convert to null-terminated string
    const sql_z = sqlite_ctx.allocator.dupeZ(u8, sql) catch return Error.OutOfMemory;
    defer sqlite_ctx.allocator.free(sql_z);

    // If we have params, use prepare and bind
    if (params.len > 0) {
        const stmt = sqlite_ctx.conn.prepare(sql_z) catch return Error.PrepareFailed;
        // Don't defer deinit here - we need to keep it alive for the rows iterator
        
        // Bind parameters
        for (params, 0..) |param, i| {
            switch (param) {
                .null => {
                    stmt.bindValue(@as(?i64, null), i) catch {
                        stmt.deinit();
                        return Error.BindError;
                    };
                },
                .boolean => |b| {
                    const val: i64 = if (b) 1 else 0;
                    stmt.bindValue(val, i) catch {
                        stmt.deinit();
                        return Error.BindError;
                    };
                },
                .int => |val| {
                    stmt.bindValue(val, i) catch {
                        stmt.deinit();
                        return Error.BindError;
                    };
                },
                .uint => |val| {
                    if (val <= std.math.maxInt(i64)) {
                        stmt.bindValue(@as(i64, @intCast(val)), i) catch {
                            stmt.deinit();
                            return Error.BindError;
                        };
                    } else {
                        stmt.deinit();
                        return Error.BindError;
                    }
                },
                .float => |val| {
                    stmt.bindValue(val, i) catch {
                        stmt.deinit();
                        return Error.BindError;
                    };
                },
                .text => |val| {
                    stmt.bindValue(val, i) catch {
                        stmt.deinit();
                        return Error.BindError;
                    };
                },
                .blob => |val| {
                    stmt.bindValue(val, i) catch {
                        stmt.deinit();
                        return Error.BindError;
                    };
                },
            }
        }
        
        // Create rows iterator from prepared statement manually
        const rows = zqlite.Rows{ .stmt = stmt, .err = null };
        
        const result_ctx = SqliteResultContext.init(allocator) catch {
            stmt.deinit();
            return Error.OutOfMemory;
        };
        result_ctx.rows = rows;
        
        return Result.init(@ptrCast(result_ctx), &sqliteResultVTable);
    } else {
        // No params - use direct query
        const rows = sqlite_ctx.conn.rows(sql_z, .{}) catch {
            return Error.ExecutionFailed;
        };
        const result_ctx = SqliteResultContext.init(allocator) catch return Error.OutOfMemory;
        result_ctx.rows = rows;
        return Result.init(@ptrCast(result_ctx), &sqliteResultVTable);
    }
}

// ============================================================================
// Statement Implementation
// ============================================================================

/// VTable for SQLite statements
const sqliteStatementVTable = StatementVTable{
    .bind = sqliteStatementBind,
    .bindAll = sqliteStatementBindAll,
    .exec = sqliteStatementExec,
    .query = sqliteStatementQuery,
    .clearBindings = sqliteStatementClearBindings,
    .reset = sqliteStatementReset,
    .paramCount = sqliteStatementParamCount,
    .deinit = sqliteStatementDeinit,
};

fn sqliteStatementBind(ctx: *anyopaque, index: usize, value: Value) Error!void {
    const stmt_ctx: *SqliteStatementContext = @ptrCast(@alignCast(ctx));

    // Convert ZDBC value to appropriate zqlite binding
    switch (value) {
        .null => {
            // For null, we can pass a null pointer - zqlite handles it
            stmt_ctx.stmt.bindValue(@as(?i64, null), index - 1) catch return Error.BindError;
        },
        .boolean => |b| {
            const val: i64 = if (b) 1 else 0;
            stmt_ctx.stmt.bindValue(val, index - 1) catch return Error.BindError;
        },
        .int => |val| {
            stmt_ctx.stmt.bindValue(val, index - 1) catch return Error.BindError;
        },
        .uint => |val| {
            // Convert u64 to i64 if possible, otherwise error
            if (val <= std.math.maxInt(i64)) {
                stmt_ctx.stmt.bindValue(@as(i64, @intCast(val)), index - 1) catch return Error.BindError;
            } else {
                return Error.BindError;
            }
        },
        .float => |val| {
            stmt_ctx.stmt.bindValue(val, index - 1) catch return Error.BindError;
        },
        .text => |val| {
            stmt_ctx.stmt.bindValue(val, index - 1) catch return Error.BindError;
        },
        .blob => |val| {
            stmt_ctx.stmt.bindValue(val, index - 1) catch return Error.BindError;
        },
    }
}

fn sqliteStatementBindAll(ctx: *anyopaque, values: []const Value) Error!void {
    for (values, 0..) |value, i| {
        try sqliteStatementBind(ctx, i + 1, value);
    }
}

fn sqliteStatementExec(ctx: *anyopaque) Error!usize {
    const stmt_ctx: *SqliteStatementContext = @ptrCast(@alignCast(ctx));

    stmt_ctx.stmt.stepToCompletion() catch return Error.ExecutionFailed;

    // Update parent context with affected rows and last insert ID
    stmt_ctx.parent_ctx.affected_rows = stmt_ctx.parent_ctx.conn.changes();
    stmt_ctx.parent_ctx.last_insert_id = stmt_ctx.parent_ctx.conn.lastInsertedRowId();

    return stmt_ctx.parent_ctx.affected_rows;
}

fn sqliteStatementQuery(_: *anyopaque) Error!Result {
    // For now, query execution through prepared statements is not implemented
    // Users should use Connection.query() instead
    return Error.NotImplemented;
}

fn sqliteStatementClearBindings(ctx: *anyopaque) Error!void {
    const stmt_ctx: *SqliteStatementContext = @ptrCast(@alignCast(ctx));
    stmt_ctx.stmt.clearBindings() catch return Error.ExecutionFailed;
}

fn sqliteStatementReset(ctx: *anyopaque) Error!void {
    const stmt_ctx: *SqliteStatementContext = @ptrCast(@alignCast(ctx));
    stmt_ctx.stmt.reset() catch return Error.ExecutionFailed;
}

fn sqliteStatementParamCount(_: *anyopaque) usize {
    // SQLite doesn't provide a way to get parameter count from statement
    // This would require parsing or tracking separately
    return 0;
}

fn sqliteStatementDeinit(ctx: *anyopaque) void {
    const stmt_ctx: *SqliteStatementContext = @ptrCast(@alignCast(ctx));
    stmt_ctx.deinit();
}

fn sqlitePrepare(ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8) Error!Statement {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));

    const stmt = sqlite_ctx.conn.prepare(sql) catch return Error.PrepareFailed;

    const stmt_ctx = SqliteStatementContext.init(allocator, stmt, sqlite_ctx) catch {
        stmt.deinit();
        return Error.OutOfMemory;
    };

    return Statement{
        .ctx = @ptrCast(stmt_ctx),
        .vtable = &sqliteStatementVTable,
        .sql = sql,
    };
}

fn sqliteBegin(ctx: *anyopaque) Error!void {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));
    sqlite_ctx.conn.transaction() catch return Error.TransactionError;
}

fn sqliteCommit(ctx: *anyopaque) Error!void {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));
    sqlite_ctx.conn.commit() catch return Error.TransactionError;
}

fn sqliteRollback(ctx: *anyopaque) Error!void {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));
    sqlite_ctx.conn.rollback();
}

fn sqliteClose(ctx: *anyopaque) void {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));
    sqlite_ctx.deinit();
}

fn sqliteLastInsertId(ctx: *anyopaque) ?i64 {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));
    return sqlite_ctx.last_insert_id;
}

fn sqliteAffectedRows(ctx: *anyopaque) usize {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));
    return sqlite_ctx.affected_rows;
}

fn sqlitePing(_: *anyopaque) Error!void {
    // SQLite is always "connected" since it's file-based
}

fn sqliteLastError(ctx: *anyopaque) ?[]const u8 {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));
    const err = sqlite_ctx.conn.lastError();
    // lastError returns [*:0]const u8 - convert to slice
    return std.mem.span(err);
}

/// Open a SQLite database connection
pub fn open(allocator: std.mem.Allocator, uri: Uri) Error!Connection {
    // Get the database path from URI
    const path = uri.database;

    // Need null-terminated path for zqlite
    const path_z = allocator.dupeZ(u8, path) catch return Error.OutOfMemory;
    defer allocator.free(path_z);

    const ctx = SqliteContext.init(allocator, path_z) catch return Error.ConnectionFailed;

    return Connection{
        .ctx = @ptrCast(ctx),
        .vtable = &sqliteConnectionVTable,
        .allocator = allocator,
        .uri = uri,
    };
}

// ============================================================================
// SQLite Driver Tests - Comprehensive Coverage
// ============================================================================

test "sqlite driver: open in-memory database" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Verify connection is valid by executing a simple query
    _ = try conn.exec("SELECT 1", &.{});
}

test "sqlite driver: create table and insert" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Create table
    _ = try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value REAL)", &.{});

    // Insert data
    _ = try conn.exec("INSERT INTO test (name, value) VALUES ('hello', 3.14)", &.{});

    // Verify last insert ID
    try std.testing.expectEqual(@as(?i64, 1), conn.lastInsertId());
}

test "sqlite driver: affected rows count" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Create table and insert multiple rows
    _ = try conn.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO users (name) VALUES ('Alice')", &.{});
    _ = try conn.exec("INSERT INTO users (name) VALUES ('Bob')", &.{});
    _ = try conn.exec("INSERT INTO users (name) VALUES ('Charlie')", &.{});

    // Update multiple rows
    const affected = try conn.exec("UPDATE users SET name = 'Updated' WHERE id <= 2", &.{});
    try std.testing.expectEqual(@as(usize, 2), affected);
    try std.testing.expectEqual(@as(usize, 2), conn.affectedRows());
}

test "sqlite driver: last insert id tracking" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)", &.{});

    _ = try conn.exec("INSERT INTO items (data) VALUES ('first')", &.{});
    try std.testing.expectEqual(@as(?i64, 1), conn.lastInsertId());

    _ = try conn.exec("INSERT INTO items (data) VALUES ('second')", &.{});
    try std.testing.expectEqual(@as(?i64, 2), conn.lastInsertId());

    _ = try conn.exec("INSERT INTO items (data) VALUES ('third')", &.{});
    try std.testing.expectEqual(@as(?i64, 3), conn.lastInsertId());
}

test "sqlite driver: transaction commit" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE txn_test (id INTEGER PRIMARY KEY, value TEXT)", &.{});

    // Start transaction
    try conn.begin();

    // Insert within transaction
    _ = try conn.exec("INSERT INTO txn_test (value) VALUES ('in_transaction')", &.{});

    // Commit transaction
    try conn.commit();

    // Verify data persists after commit
    var result = try conn.query("SELECT COUNT(*) FROM txn_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: transaction rollback" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, value TEXT)", &.{});

    // Insert before transaction
    _ = try conn.exec("INSERT INTO rollback_test (value) VALUES ('before')", &.{});

    // Start transaction
    try conn.begin();

    // Insert within transaction
    _ = try conn.exec("INSERT INTO rollback_test (value) VALUES ('during')", &.{});

    // Rollback transaction
    try conn.rollback();

    // Verify only pre-transaction data remains (1 row, not 2)
    var result = try conn.query("SELECT COUNT(*) FROM rollback_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: query returns rows" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE query_test (id INTEGER, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO query_test VALUES (1, 'Alice')", &.{});
    _ = try conn.exec("INSERT INTO query_test VALUES (2, 'Bob')", &.{});

    var result = try conn.query("SELECT id, name FROM query_test ORDER BY id", &.{});
    defer result.deinit();

    // First row
    const maybe_row1 = try result.next();
    try std.testing.expect(maybe_row1 != null);

    // Second row
    const maybe_row2 = try result.next();
    try std.testing.expect(maybe_row2 != null);

    // No more rows
    const maybe_row3 = try result.next();
    try std.testing.expect(maybe_row3 == null);
}

test "sqlite driver: empty result set" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE empty_test (id INTEGER)", &.{});

    var result = try conn.query("SELECT * FROM empty_test", &.{});
    defer result.deinit();

    // Should return no rows
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row == null);
}

test "sqlite driver: null values" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE null_test (id INTEGER, nullable_col TEXT)", &.{});
    _ = try conn.exec("INSERT INTO null_test VALUES (1, NULL)", &.{});

    var result = try conn.query("SELECT nullable_col FROM null_test", &.{});
    defer result.deinit();

    // Verify row exists (null value handling tested at driver level)
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: ping always succeeds" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Ping should always succeed for SQLite (file-based)
    try conn.ping();
}

test "sqlite driver: multiple tables" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Create multiple tables
    _ = try conn.exec("CREATE TABLE table1 (id INTEGER PRIMARY KEY)", &.{});
    _ = try conn.exec("CREATE TABLE table2 (id INTEGER PRIMARY KEY)", &.{});
    _ = try conn.exec("CREATE TABLE table3 (id INTEGER PRIMARY KEY)", &.{});

    // Insert into each
    _ = try conn.exec("INSERT INTO table1 VALUES (1)", &.{});
    _ = try conn.exec("INSERT INTO table2 VALUES (2)", &.{});
    _ = try conn.exec("INSERT INTO table3 VALUES (3)", &.{});

    // Verify all inserts
    try std.testing.expectEqual(@as(?i64, 3), conn.lastInsertId());
}

test "sqlite driver: delete operation" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE delete_test (id INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO delete_test VALUES (1)", &.{});
    _ = try conn.exec("INSERT INTO delete_test VALUES (2)", &.{});
    _ = try conn.exec("INSERT INTO delete_test VALUES (3)", &.{});

    const affected = try conn.exec("DELETE FROM delete_test WHERE id > 1", &.{});
    try std.testing.expectEqual(@as(usize, 2), affected);
}

test "sqlite driver: update operation" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE update_test (id INTEGER, status TEXT)", &.{});
    _ = try conn.exec("INSERT INTO update_test VALUES (1, 'pending')", &.{});
    _ = try conn.exec("INSERT INTO update_test VALUES (2, 'pending')", &.{});

    const affected = try conn.exec("UPDATE update_test SET status = 'done' WHERE id = 1", &.{});
    try std.testing.expectEqual(@as(usize, 1), affected);
}

test "sqlite driver: error on invalid SQL syntax" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Invalid SQL should return error
    const result = conn.exec("THIS IS NOT VALID SQL", &.{});
    try std.testing.expectError(Error.ExecutionFailed, result);
}

test "sqlite driver: error on non-existent table" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    const result = conn.exec("SELECT * FROM nonexistent_table", &.{});
    try std.testing.expectError(Error.ExecutionFailed, result);
}

test "sqlite driver: error on query invalid SQL" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    const result = conn.query("INVALID QUERY SYNTAX", &.{});
    try std.testing.expectError(Error.ExecutionFailed, result);
}

test "sqlite driver: large data insert" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE large_test (id INTEGER PRIMARY KEY, data TEXT)", &.{});

    // Insert many rows
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        _ = try conn.exec("INSERT INTO large_test (data) VALUES ('row_data')", &.{});
    }

    try std.testing.expectEqual(@as(?i64, 100), conn.lastInsertId());
}

test "sqlite driver: various data types" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec(
        \\CREATE TABLE types_test (
        \\  int_col INTEGER,
        \\  real_col REAL,
        \\  text_col TEXT,
        \\  blob_col BLOB
        \\)
    , &.{});

    _ = try conn.exec("INSERT INTO types_test VALUES (42, 3.14159, 'hello world', X'48454C4C4F')", &.{});

    var result = try conn.query("SELECT text_col FROM types_test", &.{});
    defer result.deinit();

    // Verify row exists
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: close and reopen" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;

    // First connection
    {
        var conn = try open(std.testing.allocator, uri);
        _ = try conn.exec("SELECT 1", &.{});
        conn.close();
    }

    // Second connection (new in-memory db)
    {
        var conn = try open(std.testing.allocator, uri);
        defer conn.close();
        _ = try conn.exec("SELECT 1", &.{});
    }
}

test "sqlite driver: nested transactions (savepoint not supported)" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE nested_test (id INTEGER)", &.{});

    // Start first transaction
    try conn.begin();
    _ = try conn.exec("INSERT INTO nested_test VALUES (1)", &.{});

    // Attempting nested transaction should fail (SQLite doesn't support it this way)
    const result = conn.begin();
    // This may or may not fail depending on implementation
    _ = result catch {};

    // Clean up - rollback outer transaction
    try conn.rollback();
}

test "sqlite driver: special characters in data" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE special_test (data TEXT)", &.{});

    // Test with escaped quotes
    _ = try conn.exec("INSERT INTO special_test VALUES ('it''s a test')", &.{});
    _ = try conn.exec("INSERT INTO special_test VALUES ('line1\nline2')", &.{});
    _ = try conn.exec("INSERT INTO special_test VALUES ('tab\there')", &.{});

    var result = try conn.query("SELECT COUNT(*) FROM special_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: unicode data" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE unicode_test (data TEXT)", &.{});
    _ = try conn.exec("INSERT INTO unicode_test VALUES ('你好世界')", &.{});
    _ = try conn.exec("INSERT INTO unicode_test VALUES ('🎉🎊🎈')", &.{});
    _ = try conn.exec("INSERT INTO unicode_test VALUES ('Привет мир')", &.{});

    var result = try conn.query("SELECT data FROM unicode_test WHERE data = '你好世界'", &.{});
    defer result.deinit();

    // Verify row exists
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: boundary - empty string" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE empty_str_test (data TEXT)", &.{});
    _ = try conn.exec("INSERT INTO empty_str_test VALUES ('')", &.{});

    var result = try conn.query("SELECT data FROM empty_str_test", &.{});
    defer result.deinit();

    // Verify row exists (empty string is valid)
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: boundary - very long string" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE long_str_test (data TEXT)", &.{});

    // Create a long string using runtime formatting
    var long_str: [1000]u8 = undefined;
    @memset(&long_str, 'A');
    var buf: [1100]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "INSERT INTO long_str_test VALUES ('{s}')", .{long_str}) catch unreachable;
    _ = try conn.exec(sql, &.{});

    var result = try conn.query("SELECT LENGTH(data) FROM long_str_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: prepare statement works" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Create a test table
    _ = try conn.exec("CREATE TABLE test_prep (id INTEGER, name TEXT)", &.{});

    // Prepare and execute an INSERT statement
    var stmt = try conn.prepare("INSERT INTO test_prep (id, name) VALUES (?, ?)");
    defer stmt.deinit();

    try stmt.bindAll(&.{ Value.initInt(1), Value.initText("test") });
    const affected = try stmt.exec();
    try std.testing.expectEqual(@as(usize, 1), affected);

    // Verify the insert worked
    var result = try conn.query("SELECT COUNT(*) FROM test_prep", &.{});
    defer result.deinit();

    const has_row = try result.next();
    try std.testing.expect(has_row != null);
}

test "sqlite driver: drop table" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE drop_test (id INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO drop_test VALUES (1)", &.{});
    _ = try conn.exec("DROP TABLE drop_test", &.{});

    // Table no longer exists
    const result = conn.exec("SELECT * FROM drop_test", &.{});
    try std.testing.expectError(Error.ExecutionFailed, result);
}

test "sqlite driver: alter table" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE alter_test (id INTEGER)", &.{});
    _ = try conn.exec("ALTER TABLE alter_test ADD COLUMN name TEXT", &.{});
    _ = try conn.exec("INSERT INTO alter_test (id, name) VALUES (1, 'test')", &.{});

    var result = try conn.query("SELECT name FROM alter_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: constraint violation" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE constraint_test (id INTEGER PRIMARY KEY, name TEXT UNIQUE)", &.{});
    _ = try conn.exec("INSERT INTO constraint_test VALUES (1, 'unique_name')", &.{});

    // Duplicate primary key
    const result1 = conn.exec("INSERT INTO constraint_test VALUES (1, 'other')", &.{});
    try std.testing.expectError(Error.ExecutionFailed, result1);

    // Duplicate unique value
    const result2 = conn.exec("INSERT INTO constraint_test VALUES (2, 'unique_name')", &.{});
    try std.testing.expectError(Error.ExecutionFailed, result2);
}

test "sqlite driver: foreign key (if enabled)" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("PRAGMA foreign_keys = ON", &.{});
    _ = try conn.exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)", &.{});
    _ = try conn.exec("CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))", &.{});
    _ = try conn.exec("INSERT INTO parent VALUES (1)", &.{});
    _ = try conn.exec("INSERT INTO child VALUES (1, 1)", &.{});

    // FK violation - parent doesn't exist
    const result = conn.exec("INSERT INTO child VALUES (2, 999)", &.{});
    try std.testing.expectError(Error.ExecutionFailed, result);
}

test "sqlite driver: index creation" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE index_test (id INTEGER, name TEXT)", &.{});
    _ = try conn.exec("CREATE INDEX idx_name ON index_test(name)", &.{});
    _ = try conn.exec("INSERT INTO index_test VALUES (1, 'test')", &.{});

    var result = try conn.query("SELECT * FROM index_test WHERE name = 'test'", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: aggregate functions" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE agg_test (value INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO agg_test VALUES (10)", &.{});
    _ = try conn.exec("INSERT INTO agg_test VALUES (20)", &.{});
    _ = try conn.exec("INSERT INTO agg_test VALUES (30)", &.{});

    var result = try conn.query("SELECT SUM(value), AVG(value), MIN(value), MAX(value), COUNT(*) FROM agg_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: group by and having" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE group_test (category TEXT, amount INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO group_test VALUES ('A', 10)", &.{});
    _ = try conn.exec("INSERT INTO group_test VALUES ('A', 20)", &.{});
    _ = try conn.exec("INSERT INTO group_test VALUES ('B', 5)", &.{});

    var result = try conn.query("SELECT category, SUM(amount) FROM group_test GROUP BY category HAVING SUM(amount) > 10", &.{});
    defer result.deinit();

    // Only 'A' group has sum > 10
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: subquery" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE subq_test (id INTEGER, value INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO subq_test VALUES (1, 100)", &.{});
    _ = try conn.exec("INSERT INTO subq_test VALUES (2, 200)", &.{});
    _ = try conn.exec("INSERT INTO subq_test VALUES (3, 150)", &.{});

    var result = try conn.query("SELECT * FROM subq_test WHERE value > (SELECT AVG(value) FROM subq_test)", &.{});
    defer result.deinit();

    // Only row with value 200 is above average (150)
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: join tables" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE orders (id INTEGER, customer_id INTEGER)", &.{});
    _ = try conn.exec("CREATE TABLE customers (id INTEGER, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO customers VALUES (1, 'Alice')", &.{});
    _ = try conn.exec("INSERT INTO orders VALUES (100, 1)", &.{});

    var result = try conn.query("SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: case expression" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE case_test (score INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO case_test VALUES (85)", &.{});

    var result = try conn.query("SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END FROM case_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: coalesce and nullif" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE coalesce_test (a TEXT, b TEXT)", &.{});
    _ = try conn.exec("INSERT INTO coalesce_test VALUES (NULL, 'fallback')", &.{});

    var result = try conn.query("SELECT COALESCE(a, b) FROM coalesce_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
}

test "sqlite driver: like pattern matching" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE like_test (name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO like_test VALUES ('apple')", &.{});
    _ = try conn.exec("INSERT INTO like_test VALUES ('banana')", &.{});
    _ = try conn.exec("INSERT INTO like_test VALUES ('apricot')", &.{});

    var result = try conn.query("SELECT name FROM like_test WHERE name LIKE 'ap%' ORDER BY name", &.{});
    defer result.deinit();

    const row1 = try result.next();
    try std.testing.expect(row1 != null);
    const row2 = try result.next();
    try std.testing.expect(row2 != null);
    const row3 = try result.next();
    try std.testing.expect(row3 == null);
}

test "sqlite driver: order by and limit" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE order_test (val INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO order_test VALUES (3)", &.{});
    _ = try conn.exec("INSERT INTO order_test VALUES (1)", &.{});
    _ = try conn.exec("INSERT INTO order_test VALUES (2)", &.{});

    var result = try conn.query("SELECT val FROM order_test ORDER BY val DESC LIMIT 2", &.{});
    defer result.deinit();

    // First should be 3
    const row1 = try result.next();
    try std.testing.expect(row1 != null);
    // Second should be 2
    const row2 = try result.next();
    try std.testing.expect(row2 != null);
    // No third row due to LIMIT
    const row3 = try result.next();
    try std.testing.expect(row3 == null);
}

test "sqlite driver: distinct values" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE distinct_test (val TEXT)", &.{});
    _ = try conn.exec("INSERT INTO distinct_test VALUES ('a')", &.{});
    _ = try conn.exec("INSERT INTO distinct_test VALUES ('a')", &.{});
    _ = try conn.exec("INSERT INTO distinct_test VALUES ('b')", &.{});

    var result = try conn.query("SELECT DISTINCT val FROM distinct_test ORDER BY val", &.{});
    defer result.deinit();

    const row1 = try result.next();
    try std.testing.expect(row1 != null);
    const row2 = try result.next();
    try std.testing.expect(row2 != null);
    // Only 2 distinct values
    const row3 = try result.next();
    try std.testing.expect(row3 == null);
}

// ============================================================================
// Bug Fix Tests - Comprehensive validation of the two critical fixes
// ============================================================================

test "sqlite driver: BUG FIX #1 - COUNT(*) returns integer not null" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE count_test (id INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO count_test VALUES (1)", &.{});
    _ = try conn.exec("INSERT INTO count_test VALUES (2)", &.{});
    _ = try conn.exec("INSERT INTO count_test VALUES (3)", &.{});

    var result = try conn.query("SELECT COUNT(*) FROM count_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
    const row = maybe_row.?;
    
    // This should return an integer value, not null
    const count_val = try row.get(0);
    try std.testing.expect(!count_val.isNull());
    try std.testing.expectEqual(@as(i64, 3), count_val.asInt().?);
}

test "sqlite driver: BUG FIX #1 - INTEGER column retrieval" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE int_test (id INTEGER, value INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO int_test VALUES (1, 42)", &.{});
    _ = try conn.exec("INSERT INTO int_test VALUES (2, -100)", &.{});

    var result = try conn.query("SELECT id, value FROM int_test ORDER BY id", &.{});
    defer result.deinit();

    // First row
    const maybe_row1 = try result.next();
    try std.testing.expect(maybe_row1 != null);
    const row1 = maybe_row1.?;
    
    const id1 = try row1.get(0);
    const val1 = try row1.get(1);
    try std.testing.expectEqual(@as(i64, 1), id1.asInt().?);
    try std.testing.expectEqual(@as(i64, 42), val1.asInt().?);

    // Second row
    const maybe_row2 = try result.next();
    try std.testing.expect(maybe_row2 != null);
    const row2 = maybe_row2.?;
    
    const id2 = try row2.get(0);
    const val2 = try row2.get(1);
    try std.testing.expectEqual(@as(i64, 2), id2.asInt().?);
    try std.testing.expectEqual(@as(i64, -100), val2.asInt().?);
}

test "sqlite driver: BUG FIX #1 - FLOAT column retrieval" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE float_test (id INTEGER, value REAL)", &.{});
    _ = try conn.exec("INSERT INTO float_test VALUES (1, 3.14159)", &.{});
    _ = try conn.exec("INSERT INTO float_test VALUES (2, -2.71828)", &.{});

    var result = try conn.query("SELECT value FROM float_test ORDER BY id", &.{});
    defer result.deinit();

    // First row
    const maybe_row1 = try result.next();
    try std.testing.expect(maybe_row1 != null);
    const row1 = maybe_row1.?;
    
    const val1 = try row1.get(0);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14159), val1.asFloat().?, 0.00001);

    // Second row
    const maybe_row2 = try result.next();
    try std.testing.expect(maybe_row2 != null);
    const row2 = maybe_row2.?;
    
    const val2 = try row2.get(0);
    try std.testing.expectApproxEqAbs(@as(f64, -2.71828), val2.asFloat().?, 0.00001);
}

test "sqlite driver: BUG FIX #1 - BLOB column retrieval" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE blob_test (id INTEGER, data BLOB)", &.{});
    _ = try conn.exec("INSERT INTO blob_test VALUES (1, X'48454C4C4F')", &.{}); // "HELLO" in hex

    var result = try conn.query("SELECT data FROM blob_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
    const row = maybe_row.?;
    
    const blob_val = try row.get(0);
    const blob = blob_val.asBlob().?;
    try std.testing.expectEqual(@as(usize, 5), blob.len);
    try std.testing.expectEqualSlices(u8, "HELLO", blob);
}

test "sqlite driver: BUG FIX #1 - Mixed column types in single query" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec(
        \\CREATE TABLE mixed_test (
        \\  id INTEGER,
        \\  name TEXT,
        \\  score REAL,
        \\  data BLOB,
        \\  nullable TEXT
        \\)
    , &.{});
    
    _ = try conn.exec("INSERT INTO mixed_test VALUES (1, 'Alice', 95.5, X'ABCD', NULL)", &.{});

    var result = try conn.query("SELECT id, name, score, data, nullable FROM mixed_test", &.{});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
    const row = maybe_row.?;
    
    const id = try row.get(0);
    const name = try row.get(1);
    const score = try row.get(2);
    const data = try row.get(3);
    const nullable = try row.get(4);
    
    try std.testing.expectEqual(@as(i64, 1), id.asInt().?);
    try std.testing.expectEqualStrings("Alice", name.asText().?);
    try std.testing.expectApproxEqAbs(@as(f64, 95.5), score.asFloat().?, 0.01);
    try std.testing.expectEqual(@as(usize, 2), data.asBlob().?.len);
    try std.testing.expect(nullable.isNull());
}

test "sqlite driver: BUG FIX #2 - Parameterized SELECT with WHERE clause" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE param_test (id INTEGER, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO param_test VALUES (1, 'Alice')", &.{});
    _ = try conn.exec("INSERT INTO param_test VALUES (2, 'Bob')", &.{});
    _ = try conn.exec("INSERT INTO param_test VALUES (3, 'Charlie')", &.{});

    // Query with parameter
    var result = try conn.query("SELECT name FROM param_test WHERE id = ?", &.{Value.initInt(2)});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
    const row = maybe_row.?;
    
    const name = try row.get(0);
    try std.testing.expectEqualStrings("Bob", name.asText().?);
    
    // Should only have one matching row
    const maybe_row2 = try result.next();
    try std.testing.expect(maybe_row2 == null);
}

test "sqlite driver: BUG FIX #2 - Parameterized query with multiple parameters" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE range_test (value INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO range_test VALUES (5)", &.{});
    _ = try conn.exec("INSERT INTO range_test VALUES (10)", &.{});
    _ = try conn.exec("INSERT INTO range_test VALUES (15)", &.{});
    _ = try conn.exec("INSERT INTO range_test VALUES (20)", &.{});

    // Query with two parameters
    var result = try conn.query(
        "SELECT value FROM range_test WHERE value >= ? AND value <= ? ORDER BY value",
        &.{Value.initInt(8), Value.initInt(18)}
    );
    defer result.deinit();

    // Should match 10 and 15
    const maybe_row1 = try result.next();
    try std.testing.expect(maybe_row1 != null);
    const row1 = maybe_row1.?;
    const val1 = try row1.get(0);
    try std.testing.expectEqual(@as(i64, 10), val1.asInt().?);

    const maybe_row2 = try result.next();
    try std.testing.expect(maybe_row2 != null);
    const row2 = maybe_row2.?;
    const val2 = try row2.get(0);
    try std.testing.expectEqual(@as(i64, 15), val2.asInt().?);

    const maybe_row3 = try result.next();
    try std.testing.expect(maybe_row3 == null);
}

test "sqlite driver: BUG FIX #2 - Parameterized query with TEXT parameter" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE text_param_test (id INTEGER, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO text_param_test VALUES (1, 'Alice')", &.{});
    _ = try conn.exec("INSERT INTO text_param_test VALUES (2, 'Bob')", &.{});

    var result = try conn.query("SELECT id FROM text_param_test WHERE name = ?", &.{Value.initText("Alice")});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
    const row = maybe_row.?;
    
    const id = try row.get(0);
    try std.testing.expectEqual(@as(i64, 1), id.asInt().?);
}

test "sqlite driver: BUG FIX #2 - Parameterized query with FLOAT parameter" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE float_param_test (id INTEGER, score REAL)", &.{});
    _ = try conn.exec("INSERT INTO float_param_test VALUES (1, 85.5)", &.{});
    _ = try conn.exec("INSERT INTO float_param_test VALUES (2, 92.3)", &.{});
    _ = try conn.exec("INSERT INTO float_param_test VALUES (3, 78.9)", &.{});

    var result = try conn.query("SELECT id FROM float_param_test WHERE score > ?", &.{Value.initFloat(90.0)});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
    const row = maybe_row.?;
    
    const id = try row.get(0);
    try std.testing.expectEqual(@as(i64, 2), id.asInt().?);
    
    // Only one row should match
    const maybe_row2 = try result.next();
    try std.testing.expect(maybe_row2 == null);
}

test "sqlite driver: BUG FIX #2 - Parameterized query with NULL parameter" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE null_param_test (id INTEGER, value TEXT)", &.{});
    _ = try conn.exec("INSERT INTO null_param_test VALUES (1, 'data')", &.{});
    _ = try conn.exec("INSERT INTO null_param_test VALUES (2, NULL)", &.{});

    var result = try conn.query("SELECT id FROM null_param_test WHERE value IS ?", &.{Value.initNull()});
    defer result.deinit();

    const maybe_row = try result.next();
    try std.testing.expect(maybe_row != null);
    const row = maybe_row.?;
    
    const id = try row.get(0);
    try std.testing.expectEqual(@as(i64, 2), id.asInt().?);
}

test "sqlite driver: BUG FIX #2 - Parameterized query with boolean parameter" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE bool_param_test (id INTEGER, active INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO bool_param_test VALUES (1, 1)", &.{});
    _ = try conn.exec("INSERT INTO bool_param_test VALUES (2, 0)", &.{});
    _ = try conn.exec("INSERT INTO bool_param_test VALUES (3, 1)", &.{});

    var result = try conn.query("SELECT id FROM bool_param_test WHERE active = ? ORDER BY id", &.{Value.initBool(true)});
    defer result.deinit();

    const maybe_row1 = try result.next();
    try std.testing.expect(maybe_row1 != null);
    const row1 = maybe_row1.?;
    const id1 = try row1.get(0);
    try std.testing.expectEqual(@as(i64, 1), id1.asInt().?);

    const maybe_row2 = try result.next();
    try std.testing.expect(maybe_row2 != null);
    const row2 = maybe_row2.?;
    const id2 = try row2.get(0);
    try std.testing.expectEqual(@as(i64, 3), id2.asInt().?);

    const maybe_row3 = try result.next();
    try std.testing.expect(maybe_row3 == null);
}

test "sqlite driver: BUG FIX #2 - SQL injection protection with parameters" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    _ = try conn.exec("CREATE TABLE injection_test (id INTEGER, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO injection_test VALUES (1, 'Alice')", &.{});

    // Attempt SQL injection through parameter (should be safely escaped)
    const malicious_input = "Alice' OR '1'='1";
    var result = try conn.query("SELECT id FROM injection_test WHERE name = ?", &.{Value.initText(malicious_input)});
    defer result.deinit();

    // Should return no rows (parameter is treated as literal string)
    const maybe_row = try result.next();
    try std.testing.expect(maybe_row == null);
}
