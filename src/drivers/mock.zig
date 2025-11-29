//! Mock database driver for testing
//!
//! This driver provides an in-memory database implementation
//! useful for testing the ZDBC interface without actual database connections.

const std = @import("std");
const Connection = @import("../connection.zig").Connection;
const ConnectionVTable = @import("../connection.zig").ConnectionVTable;
const Result = @import("../result.zig").Result;
const ResultVTable = @import("../result.zig").ResultVTable;
const Statement = @import("../statement.zig").Statement;
const StatementVTable = @import("../statement.zig").StatementVTable;
const Value = @import("../value.zig").Value;
const Error = @import("../error.zig").Error;
const Uri = @import("../uri.zig").Uri;

/// Mock database context
pub const MockContext = struct {
    allocator: std.mem.Allocator,
    in_transaction: bool = false,
    last_insert_id: i64 = 0,
    affected_rows: usize = 0,
    last_error: ?[]const u8 = null,
    closed: bool = false,

    /// Tables stored in memory
    tables: std.StringHashMap(MockTable),

    /// Create a new mock context
    pub fn init(allocator: std.mem.Allocator) !*MockContext {
        const ctx = try allocator.create(MockContext);
        ctx.* = MockContext{
            .allocator = allocator,
            .tables = std.StringHashMap(MockTable).init(allocator),
        };
        return ctx;
    }

    /// Free the mock context
    pub fn deinit(self: *MockContext) void {
        var it = self.tables.valueIterator();
        while (it.next()) |table| {
            var t = table.*;
            t.deinit();
        }
        self.tables.deinit();
        self.allocator.destroy(self);
    }
};

/// Mock table structure
pub const MockTable = struct {
    name: []const u8,
    columns: std.ArrayList([]const u8),
    rows: std.ArrayList(std.ArrayList(Value)),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, name: []const u8) MockTable {
        return MockTable{
            .name = name,
            .columns = std.ArrayList([]const u8).init(allocator),
            .rows = std.ArrayList(std.ArrayList(Value)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MockTable) void {
        self.columns.deinit(self.allocator);
        for (self.rows.items) |*row| {
            row.deinit(self.allocator);
        }
        self.rows.deinit(self.allocator);
    }
};

/// Mock result context
pub const MockResultContext = struct {
    allocator: std.mem.Allocator,
    columns: []const []const u8,
    rows: []const []const Value,
    current_row: usize = 0,
    affected: usize = 0,

    pub fn init(allocator: std.mem.Allocator) !*MockResultContext {
        const ctx = try allocator.create(MockResultContext);
        ctx.* = MockResultContext{
            .allocator = allocator,
            .columns = &.{},
            .rows = &.{},
        };
        return ctx;
    }

    pub fn deinit(self: *MockResultContext) void {
        self.allocator.destroy(self);
    }
};

/// VTable for mock results
const mockResultVTable = ResultVTable{
    .next = mockResultNext,
    .columnCount = mockResultColumnCount,
    .columnName = mockResultColumnName,
    .getValue = mockResultGetValue,
    .getValueByName = mockResultGetValueByName,
    .affectedRows = mockResultAffectedRows,
    .reset = mockResultReset,
    .deinit = mockResultDeinit,
};

fn mockResultNext(ctx: *anyopaque) Error!bool {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.current_row < result_ctx.rows.len) {
        result_ctx.current_row += 1;
        return true;
    }
    return false;
}

fn mockResultColumnCount(ctx: *anyopaque) usize {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    return result_ctx.columns.len;
}

fn mockResultColumnName(ctx: *anyopaque, index: usize) ?[]const u8 {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    if (index < result_ctx.columns.len) {
        return result_ctx.columns[index];
    }
    return null;
}

fn mockResultGetValue(ctx: *anyopaque, index: usize) Error!Value {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    if (result_ctx.current_row == 0 or result_ctx.current_row > result_ctx.rows.len) {
        return Error.NoMoreRows;
    }
    const row = result_ctx.rows[result_ctx.current_row - 1];
    if (index >= row.len) {
        return Error.ColumnOutOfBounds;
    }
    return row[index];
}

fn mockResultGetValueByName(ctx: *anyopaque, name: []const u8) Error!Value {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    for (result_ctx.columns, 0..) |col, i| {
        if (std.mem.eql(u8, col, name)) {
            return mockResultGetValue(ctx, i);
        }
    }
    return Error.ColumnNotFound;
}

fn mockResultAffectedRows(ctx: *anyopaque) usize {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    return result_ctx.affected;
}

fn mockResultReset(ctx: *anyopaque) Error!void {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    result_ctx.current_row = 0;
}

fn mockResultDeinit(ctx: *anyopaque) void {
    const result_ctx: *MockResultContext = @ptrCast(@alignCast(ctx));
    result_ctx.deinit();
}

/// VTable for mock connections
pub const mockConnectionVTable = ConnectionVTable{
    .exec = mockExec,
    .query = mockQuery,
    .prepare = mockPrepare,
    .begin = mockBegin,
    .commit = mockCommit,
    .rollback = mockRollback,
    .close = mockClose,
    .lastInsertId = mockLastInsertId,
    .affectedRows = mockAffectedRows,
    .ping = mockPing,
    .lastError = mockLastError,
};

fn mockExec(ctx: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const Value) Error!usize {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    if (mock_ctx.closed) return Error.NotConnected;
    mock_ctx.affected_rows = 1;
    mock_ctx.last_insert_id += 1;
    return 1;
}

fn mockQuery(ctx: *anyopaque, allocator: std.mem.Allocator, _: []const u8, _: []const Value) Error!Result {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    if (mock_ctx.closed) return Error.NotConnected;

    const result_ctx = MockResultContext.init(allocator) catch return Error.OutOfMemory;
    return Result.init(@ptrCast(result_ctx), &mockResultVTable);
}

fn mockPrepare(_: *anyopaque, _: std.mem.Allocator, _: []const u8) Error!Statement {
    return Error.NotImplemented;
}

fn mockBegin(ctx: *anyopaque) Error!void {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    if (mock_ctx.closed) return Error.NotConnected;
    if (mock_ctx.in_transaction) return Error.TransactionError;
    mock_ctx.in_transaction = true;
}

fn mockCommit(ctx: *anyopaque) Error!void {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    if (mock_ctx.closed) return Error.NotConnected;
    if (!mock_ctx.in_transaction) return Error.TransactionError;
    mock_ctx.in_transaction = false;
}

fn mockRollback(ctx: *anyopaque) Error!void {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    if (mock_ctx.closed) return Error.NotConnected;
    mock_ctx.in_transaction = false;
}

fn mockClose(ctx: *anyopaque) void {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    mock_ctx.closed = true;
    mock_ctx.deinit();
}

fn mockLastInsertId(ctx: *anyopaque) ?i64 {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    return mock_ctx.last_insert_id;
}

fn mockAffectedRows(ctx: *anyopaque) usize {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    return mock_ctx.affected_rows;
}

fn mockPing(ctx: *anyopaque) Error!void {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    if (mock_ctx.closed) return Error.NotConnected;
}

fn mockLastError(ctx: *anyopaque) ?[]const u8 {
    const mock_ctx: *MockContext = @ptrCast(@alignCast(ctx));
    return mock_ctx.last_error;
}

/// Open a mock database connection
pub fn open(allocator: std.mem.Allocator, uri: Uri) Error!Connection {
    const ctx = MockContext.init(allocator) catch return Error.OutOfMemory;
    return Connection{
        .ctx = @ptrCast(ctx),
        .vtable = &mockConnectionVTable,
        .allocator = allocator,
        .uri = uri,
    };
}

test "mock driver basic operations" {
    const allocator = std.testing.allocator;

    const uri = try Uri.parse("mock://localhost/test");
    var conn = try open(allocator, uri);
    defer conn.close();

    // Test ping
    try conn.ping();

    // Test exec
    const affected = try conn.exec("INSERT INTO test VALUES (1)", &.{});
    try std.testing.expectEqual(@as(usize, 1), affected);

    // Test transaction
    try conn.begin();
    try conn.commit();

    // Test query
    var result = try conn.query("SELECT * FROM test", &.{});
    defer result.deinit();
}

test "mock driver transactions" {
    const allocator = std.testing.allocator;

    const uri = try Uri.parse("mock://localhost/test");
    var conn = try open(allocator, uri);
    defer conn.close();

    // Begin transaction
    try conn.begin();

    // Nested begin should fail
    try std.testing.expectError(Error.TransactionError, conn.begin());

    // Commit
    try conn.commit();

    // Commit without transaction should fail
    try std.testing.expectError(Error.TransactionError, conn.commit());

    // Rollback without transaction is ok
    try conn.rollback();
}
