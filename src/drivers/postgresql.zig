//! PostgreSQL database driver
//!
//! This driver wraps the pg.zig library to provide PostgreSQL support
//! through the ZDBC VTable interface.
//!
//! Dependencies: https://github.com/karlseguin/pg.zig

const std = @import("std");
const pg = @import("pg");
const Connection = @import("../connection.zig").Connection;
const ConnectionVTable = @import("../connection.zig").ConnectionVTable;
const Result = @import("../result.zig").Result;
const ResultVTable = @import("../result.zig").ResultVTable;
const Statement = @import("../statement.zig").Statement;
const StatementVTable = @import("../statement.zig").StatementVTable;
const Value = @import("../value.zig").Value;
const Error = @import("../error.zig").Error;
const Uri = @import("../uri.zig").Uri;

/// PostgreSQL connection context
pub const PgContext = struct {
    allocator: std.mem.Allocator,
    conn: pg.Conn,
    last_error: ?[]const u8 = null,
    affected_rows: usize = 0,
    last_insert_id: i64 = 0,

    pub fn init(allocator: std.mem.Allocator, uri: Uri) !*PgContext {
        const host = uri.host orelse "127.0.0.1";
        const port = uri.port orelse 5432;

        var conn = pg.Conn.open(allocator, .{
            .host = host,
            .port = port,
        }) catch return error.ConnectionFailed;

        // Authenticate
        conn.auth(.{
            .username = uri.username orelse "postgres",
            .password = uri.password,
            .database = if (uri.database.len > 0) uri.database else null,
        }) catch return error.ConnectionFailed;

        const ctx = try allocator.create(PgContext);
        ctx.* = PgContext{
            .allocator = allocator,
            .conn = conn,
        };
        return ctx;
    }

    pub fn deinit(self: *PgContext) void {
        self.conn.deinit();
        self.allocator.destroy(self);
    }
};

/// PostgreSQL result context
pub const PgResultContext = struct {
    allocator: std.mem.Allocator,
    result: ?*pg.Result = null,
    current_row: ?pg.Row = null,

    pub fn init(allocator: std.mem.Allocator) !*PgResultContext {
        const ctx = try allocator.create(PgResultContext);
        ctx.* = PgResultContext{
            .allocator = allocator,
        };
        return ctx;
    }

    pub fn deinit(self: *PgResultContext) void {
        if (self.result) |result| {
            result.deinit();
        }
        self.allocator.destroy(self);
    }
};

/// VTable for PostgreSQL results
const pgResultVTable = ResultVTable{
    .next = pgResultNext,
    .columnCount = pgResultColumnCount,
    .columnName = pgResultColumnName,
    .getValue = pgResultGetValue,
    .getValueByName = pgResultGetValueByName,
    .affectedRows = pgResultAffectedRows,
    .reset = null,
    .deinit = pgResultDeinit,
};

fn pgResultNext(ctx: *anyopaque) Error!bool {
    const result_ctx: *PgResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.result) |result| {
        const row = result.next() catch return Error.ExecutionFailed;
        if (row) |r| {
            result_ctx.current_row = r;
            return true;
        }
    }
    return false;
}

fn pgResultColumnCount(ctx: *anyopaque) usize {
    const result_ctx: *PgResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.result) |result| {
        return result.number_of_columns;
    }
    return 0;
}

fn pgResultColumnName(_: *anyopaque, _: usize) ?[]const u8 {
    return null;
}

fn pgResultGetValue(ctx: *anyopaque, index: usize) Error!Value {
    const result_ctx: *PgResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.current_row) |row| {
        if (row.get(?[]const u8, index)) |text| {
            return Value.initText(text);
        }
        return Value.initNull();
    }
    return Error.NoMoreRows;
}

fn pgResultGetValueByName(_: *anyopaque, _: []const u8) Error!Value {
    return Error.NotImplemented;
}

fn pgResultAffectedRows(_: *anyopaque) usize {
    return 0;
}

fn pgResultDeinit(ctx: *anyopaque) void {
    const result_ctx: *PgResultContext = @ptrCast(@alignCast(ctx));
    result_ctx.deinit();
}

/// VTable for PostgreSQL connections
pub const pgConnectionVTable = ConnectionVTable{
    .exec = pgExec,
    .query = pgQuery,
    .prepare = pgPrepare,
    .begin = pgBegin,
    .commit = pgCommit,
    .rollback = pgRollback,
    .close = pgClose,
    .lastInsertId = pgLastInsertId,
    .affectedRows = pgAffectedRows,
    .ping = pgPing,
    .lastError = pgLastError,
};

fn pgExec(ctx: *anyopaque, _: std.mem.Allocator, sql: []const u8, params: []const Value) Error!usize {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    _ = params;

    const affected = pg_ctx.conn.exec(sql, .{}) catch return Error.ExecutionFailed;
    // exec returns ?i64, need to convert to usize
    if (affected) |count| {
        pg_ctx.affected_rows = if (count >= 0) @intCast(count) else 0;
    } else {
        pg_ctx.affected_rows = 0;
    }
    return pg_ctx.affected_rows;
}

fn pgQuery(ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8, params: []const Value) Error!Result {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    _ = params;

    const result = pg_ctx.conn.query(sql, .{}) catch return Error.ExecutionFailed;

    const result_ctx = PgResultContext.init(allocator) catch return Error.OutOfMemory;
    result_ctx.result = result;

    return Result.init(@ptrCast(result_ctx), &pgResultVTable);
}

fn pgPrepare(_: *anyopaque, _: std.mem.Allocator, _: []const u8) Error!Statement {
    return Error.NotImplemented;
}

fn pgBegin(ctx: *anyopaque) Error!void {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    pg_ctx.conn.begin() catch return Error.TransactionError;
}

fn pgCommit(ctx: *anyopaque) Error!void {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    pg_ctx.conn.commit() catch return Error.TransactionError;
}

fn pgRollback(ctx: *anyopaque) Error!void {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    pg_ctx.conn.rollback() catch return Error.TransactionError;
}

fn pgClose(ctx: *anyopaque) void {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    pg_ctx.deinit();
}

fn pgLastInsertId(ctx: *anyopaque) ?i64 {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    return pg_ctx.last_insert_id;
}

fn pgAffectedRows(ctx: *anyopaque) usize {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    return pg_ctx.affected_rows;
}

fn pgPing(_: *anyopaque) Error!void {
    // PostgreSQL ping would require executing a simple query
}

fn pgLastError(ctx: *anyopaque) ?[]const u8 {
    const pg_ctx: *PgContext = @ptrCast(@alignCast(ctx));
    if (pg_ctx.conn.err) |err| {
        return err.message;
    }
    return null;
}

/// Open a PostgreSQL database connection
pub fn open(allocator: std.mem.Allocator, uri: Uri) Error!Connection {
    const ctx = PgContext.init(allocator, uri) catch return Error.ConnectionFailed;

    return Connection{
        .ctx = @ptrCast(ctx),
        .vtable = &pgConnectionVTable,
        .allocator = allocator,
        .uri = uri,
    };
}

test "postgresql driver interface" {
    // This test only verifies the interface compiles correctly
    // Actual PostgreSQL tests require a running database
    const uri = Uri.parse("postgresql://user:pass@localhost:5432/testdb") catch unreachable;
    _ = uri;
}
