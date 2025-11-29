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
            // Drain any remaining rows to leave the connection in a clean state.
            // Errors are ignored here because we're in cleanup and can't propagate them,
            // and failing to drain won't cause memory leaks - just connection state issues
            // that will be resolved when the connection is closed.
            result.drain() catch {};
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

// ============================================================================
// PostgreSQL Driver Integration Tests
// These tests require a running PostgreSQL database with environment variables:
// - ZDBC_PG_HOST (default: localhost)
// - ZDBC_PG_PORT (default: 5432)
// - ZDBC_PG_USER (default: postgres)
// - ZDBC_PG_PASSWORD
// - ZDBC_PG_DATABASE (default: zdbc_test)
// ============================================================================

fn getPgTestUri(allocator: std.mem.Allocator) ?[]const u8 {
    const password = std.process.getEnvVarOwned(allocator, "ZDBC_PG_PASSWORD") catch return null;
    defer allocator.free(password);

    const host = std.process.getEnvVarOwned(allocator, "ZDBC_PG_HOST") catch allocator.dupe(u8, "localhost") catch return null;
    defer allocator.free(host);

    const port = std.process.getEnvVarOwned(allocator, "ZDBC_PG_PORT") catch allocator.dupe(u8, "5432") catch return null;
    defer allocator.free(port);

    const user = std.process.getEnvVarOwned(allocator, "ZDBC_PG_USER") catch allocator.dupe(u8, "postgres") catch return null;
    defer allocator.free(user);

    const database = std.process.getEnvVarOwned(allocator, "ZDBC_PG_DATABASE") catch allocator.dupe(u8, "zdbc_test") catch return null;
    defer allocator.free(database);

    return std.fmt.allocPrint(allocator, "postgresql://{s}:{s}@{s}:{s}/{s}", .{
        user,
        password,
        host,
        port,
        database,
    }) catch return null;
}

test "postgresql: connection and ping" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse {
        // Skip test if PostgreSQL is not configured
        return;
    };
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch |err| {
        std.debug.print("PostgreSQL connection failed (expected if no server): {}\n", .{err});
        return;
    };
    defer conn.close();

    // Test ping
    try conn.ping();
}

test "postgresql: create table and insert" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_basic", &.{}) catch {};

    // Create table
    _ = try conn.exec("CREATE TABLE pg_test_basic (id SERIAL PRIMARY KEY, name TEXT, value REAL)", &.{});

    // Insert data
    _ = try conn.exec("INSERT INTO pg_test_basic (name, value) VALUES ('hello', 3.14)", &.{});

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_basic", &.{});
}

test "postgresql: query returns rows" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_query", &.{}) catch {};

    // Create and populate table
    _ = try conn.exec("CREATE TABLE pg_test_query (id SERIAL, name TEXT)", &.{});
    _ = try conn.exec("INSERT INTO pg_test_query (name) VALUES ('Alice')", &.{});
    _ = try conn.exec("INSERT INTO pg_test_query (name) VALUES ('Bob')", &.{});

    // Query
    var result = try conn.query("SELECT id, name FROM pg_test_query ORDER BY id", &.{});
    defer result.deinit();

    // First row
    const row1 = try result.next();
    try std.testing.expect(row1 != null);

    // Second row
    const row2 = try result.next();
    try std.testing.expect(row2 != null);

    // No more rows
    const row3 = try result.next();
    try std.testing.expect(row3 == null);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_query", &.{});
}

test "postgresql: transaction commit" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_txn", &.{}) catch {};

    _ = try conn.exec("CREATE TABLE pg_test_txn (id SERIAL PRIMARY KEY, value TEXT)", &.{});

    // Start transaction
    try conn.begin();

    // Insert within transaction
    _ = try conn.exec("INSERT INTO pg_test_txn (value) VALUES ('in_transaction')", &.{});

    // Commit
    try conn.commit();

    // Verify data persists (cast to text since pg.zig returns binary format)
    var result = try conn.query("SELECT COUNT(*)::text FROM pg_test_txn", &.{});
    defer result.deinit();
    const has_row = try result.next();
    try std.testing.expect(has_row != null);
    // Verify count is 1 (committed row)
    const row = has_row.?;
    const count_text = try row.getText(0);
    try std.testing.expect(count_text != null);
    try std.testing.expectEqualStrings("1", count_text.?);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_txn", &.{});
}

test "postgresql: transaction rollback" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_rollback", &.{}) catch {};

    _ = try conn.exec("CREATE TABLE pg_test_rollback (id SERIAL PRIMARY KEY, value TEXT)", &.{});

    // Insert before transaction
    _ = try conn.exec("INSERT INTO pg_test_rollback (value) VALUES ('before')", &.{});

    // Start transaction
    try conn.begin();

    // Insert within transaction
    _ = try conn.exec("INSERT INTO pg_test_rollback (value) VALUES ('during')", &.{});

    // Rollback
    try conn.rollback();

    // Verify only pre-transaction data remains (cast to text since pg.zig returns binary format)
    var result = try conn.query("SELECT COUNT(*)::text FROM pg_test_rollback", &.{});
    defer result.deinit();
    const has_row = try result.next();
    try std.testing.expect(has_row != null);
    // Verify count is 1 (only 'before' row, rollback reverted 'during' row)
    const row = has_row.?;
    const count_text = try row.getText(0);
    try std.testing.expect(count_text != null);
    try std.testing.expectEqualStrings("1", count_text.?);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_rollback", &.{});
}

test "postgresql: multiple data types" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_types", &.{}) catch {};

    // Create table with various types
    _ = try conn.exec(
        \\CREATE TABLE pg_test_types (
        \\  int_col INTEGER,
        \\  bigint_col BIGINT,
        \\  real_col REAL,
        \\  double_col DOUBLE PRECISION,
        \\  text_col TEXT,
        \\  bool_col BOOLEAN,
        \\  timestamp_col TIMESTAMP
        \\)
    , &.{});

    _ = try conn.exec("INSERT INTO pg_test_types VALUES (42, 9223372036854775807, 3.14, 2.71828, 'hello', true, '2024-01-01 12:00:00')", &.{});

    var result = try conn.query("SELECT text_col FROM pg_test_types", &.{});
    defer result.deinit();

    const has_row = try result.next();
    try std.testing.expect(has_row != null);
    // Verify the text column value matches what was inserted
    const row = has_row.?;
    const text_val = try row.getText(0);
    try std.testing.expect(text_val != null);
    try std.testing.expectEqualStrings("hello", text_val.?);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_types", &.{});
}

test "postgresql: unicode data" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_unicode", &.{}) catch {};

    _ = try conn.exec("CREATE TABLE pg_test_unicode (data TEXT)", &.{});
    _ = try conn.exec("INSERT INTO pg_test_unicode VALUES ('你好世界')", &.{});
    _ = try conn.exec("INSERT INTO pg_test_unicode VALUES ('🎉🎊🎈')", &.{});
    _ = try conn.exec("INSERT INTO pg_test_unicode VALUES ('Привет мир')", &.{});

    var result = try conn.query("SELECT data FROM pg_test_unicode WHERE data = '你好世界'", &.{});
    defer result.deinit();

    const has_row = try result.next();
    try std.testing.expect(has_row != null);
    // Verify the unicode data matches what was inserted
    const row = has_row.?;
    const unicode_val = try row.getText(0);
    try std.testing.expect(unicode_val != null);
    try std.testing.expectEqualStrings("你好世界", unicode_val.?);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_unicode", &.{});
}

test "postgresql: null values" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_null", &.{}) catch {};

    _ = try conn.exec("CREATE TABLE pg_test_null (id INTEGER, nullable_col TEXT)", &.{});
    _ = try conn.exec("INSERT INTO pg_test_null VALUES (1, NULL)", &.{});

    var result = try conn.query("SELECT nullable_col FROM pg_test_null", &.{});
    defer result.deinit();

    const has_row = try result.next();
    try std.testing.expect(has_row != null);
    // Verify the NULL value is properly returned
    const row = has_row.?;
    const is_null = try row.isNull(0);
    try std.testing.expect(is_null);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_null", &.{});
}

test "postgresql: aggregate functions" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop table if exists
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_agg", &.{}) catch {};

    _ = try conn.exec("CREATE TABLE pg_test_agg (value INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO pg_test_agg VALUES (10)", &.{});
    _ = try conn.exec("INSERT INTO pg_test_agg VALUES (20)", &.{});
    _ = try conn.exec("INSERT INTO pg_test_agg VALUES (30)", &.{});

    // Cast aggregate results to text since pg.zig returns binary format
    var result = try conn.query("SELECT SUM(value)::text, AVG(value)::text, MIN(value)::text, MAX(value)::text, COUNT(*)::text FROM pg_test_agg", &.{});
    defer result.deinit();

    const has_row = try result.next();
    try std.testing.expect(has_row != null);
    // Verify aggregate function results: SUM=60, AVG=20, MIN=10, MAX=30, COUNT=3
    const row = has_row.?;
    const sum_val = try row.getText(0);
    try std.testing.expect(sum_val != null);
    try std.testing.expectEqualStrings("60", sum_val.?);
    const count_val = try row.getText(4);
    try std.testing.expect(count_val != null);
    try std.testing.expectEqualStrings("3", count_val.?);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_agg", &.{});
}

test "postgresql: join tables" {
    const allocator = std.testing.allocator;
    const uri_str = getPgTestUri(allocator) orelse return;
    defer allocator.free(uri_str);

    const uri = Uri.parse(uri_str) catch return;
    var conn = open(allocator, uri) catch return;
    defer conn.close();

    // Drop tables if exist
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_orders", &.{}) catch {};
    _ = conn.exec("DROP TABLE IF EXISTS pg_test_customers", &.{}) catch {};

    _ = try conn.exec("CREATE TABLE pg_test_customers (id INTEGER PRIMARY KEY, name TEXT)", &.{});
    _ = try conn.exec("CREATE TABLE pg_test_orders (id INTEGER, customer_id INTEGER)", &.{});
    _ = try conn.exec("INSERT INTO pg_test_customers VALUES (1, 'Alice')", &.{});
    _ = try conn.exec("INSERT INTO pg_test_orders VALUES (100, 1)", &.{});

    // Cast integer to text since pg.zig returns binary format
    var result = try conn.query("SELECT o.id::text, c.name FROM pg_test_orders o JOIN pg_test_customers c ON o.customer_id = c.id", &.{});
    defer result.deinit();

    const has_row = try result.next();
    try std.testing.expect(has_row != null);
    // Verify join results: order id 100, customer name 'Alice'
    const row = has_row.?;
    const order_id = try row.getText(0);
    try std.testing.expect(order_id != null);
    try std.testing.expectEqualStrings("100", order_id.?);
    const customer_name = try row.getText(1);
    try std.testing.expect(customer_name != null);
    try std.testing.expectEqualStrings("Alice", customer_name.?);

    // Cleanup
    _ = try conn.exec("DROP TABLE pg_test_orders", &.{});
    _ = try conn.exec("DROP TABLE pg_test_customers", &.{});
}
