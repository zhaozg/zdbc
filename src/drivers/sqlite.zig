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
    // zqlite doesn't expose column names directly in a simple way
    return null;
}

fn sqliteResultGetValue(ctx: *anyopaque, index: usize) Error!Value {
    const result_ctx: *SqliteResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.current_row) |row| {
        // Check if null
        if (row.nullableText(index)) |text| {
            return Value.initText(text);
        }
        return Value.initNull();
    }
    return Error.NoMoreRows;
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

    // Convert to null-terminated string
    const sql_z = sqlite_ctx.allocator.dupeZ(u8, sql) catch return Error.OutOfMemory;
    defer sqlite_ctx.allocator.free(sql_z);

    // Execute with no params for now (zqlite uses different param binding)
    _ = params;
    sqlite_ctx.conn.execNoArgs(sql_z) catch {
        return Error.ExecutionFailed;
    };

    sqlite_ctx.affected_rows = sqlite_ctx.conn.changes();
    sqlite_ctx.last_insert_id = sqlite_ctx.conn.lastInsertedRowId();
    return sqlite_ctx.affected_rows;
}

fn sqliteQuery(ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8, params: []const Value) Error!Result {
    const sqlite_ctx: *SqliteContext = @ptrCast(@alignCast(ctx));

    // Convert to null-terminated string
    const sql_z = sqlite_ctx.allocator.dupeZ(u8, sql) catch return Error.OutOfMemory;
    defer sqlite_ctx.allocator.free(sql_z);

    _ = params;
    const rows = sqlite_ctx.conn.rows(sql_z, .{}) catch {
        return Error.ExecutionFailed;
    };

    const result_ctx = SqliteResultContext.init(allocator) catch return Error.OutOfMemory;
    result_ctx.rows = rows;

    return Result.init(@ptrCast(result_ctx), &sqliteResultVTable);
}

fn sqlitePrepare(_: *anyopaque, _: std.mem.Allocator, _: []const u8) Error!Statement {
    return Error.NotImplemented;
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

test "sqlite driver open" {
    const uri = Uri.parse("sqlite://:memory:") catch unreachable;
    var conn = try open(std.testing.allocator, uri);
    defer conn.close();

    // Test basic operation
    _ = try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO test (name) VALUES ('hello')", &.{});

    try std.testing.expectEqual(@as(?i64, 1), conn.lastInsertId());
}
