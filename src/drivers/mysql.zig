//! MySQL database driver
//!
//! This driver wraps the myzql library to provide MySQL/MariaDB support
//! through the ZDBC VTable interface.
//!
//! Dependencies: https://github.com/speed2exe/myzql

const std = @import("std");
const myzql = @import("myzql");
const Connection = @import("../connection.zig").Connection;
const ConnectionVTable = @import("../connection.zig").ConnectionVTable;
const Result = @import("../result.zig").Result;
const ResultVTable = @import("../result.zig").ResultVTable;
const Statement = @import("../statement.zig").Statement;
const StatementVTable = @import("../statement.zig").StatementVTable;
const Value = @import("../value.zig").Value;
const Error = @import("../error.zig").Error;
const Uri = @import("../uri.zig").Uri;

/// MySQL connection context
pub const MysqlContext = struct {
    allocator: std.mem.Allocator,
    conn: myzql.conn.Conn,
    last_error: ?[]const u8 = null,
    affected_rows: usize = 0,
    last_insert_id: i64 = 0,
    // Store allocated strings for config
    username_z: [:0]const u8,
    database_z: [:0]const u8,

    pub fn init(allocator: std.mem.Allocator, uri: Uri) !*MysqlContext {
        const host = uri.host orelse "127.0.0.1";
        const port = uri.port orelse 3306;

        // Parse host to IP address
        var addr_bytes: [4]u8 = .{ 127, 0, 0, 1 };
        if (!std.mem.eql(u8, host, "localhost") and !std.mem.eql(u8, host, "127.0.0.1")) {
            // Simple IP parsing for common cases
            var parts = std.mem.splitScalar(u8, host, '.');
            var i: usize = 0;
            while (parts.next()) |part| : (i += 1) {
                if (i >= 4) break;
                addr_bytes[i] = std.fmt.parseInt(u8, part, 10) catch 0;
            }
        }

        // Allocate null-terminated strings for myzql config
        const username_z = allocator.dupeZ(u8, uri.username orelse "root") catch return error.OutOfMemory;
        errdefer allocator.free(username_z);
        const database_z = allocator.dupeZ(u8, if (uri.database.len > 0) uri.database else "") catch return error.OutOfMemory;
        errdefer allocator.free(database_z);

        const config = myzql.config.Config{
            .username = username_z,
            .password = uri.password orelse "",
            .database = database_z,
            .address = std.net.Address.initIp4(addr_bytes, port),
        };

        var conn = myzql.conn.Conn.init(allocator, &config) catch return error.ConnectionFailed;

        // Ping to verify connection
        conn.ping() catch return error.ConnectionFailed;

        const ctx = try allocator.create(MysqlContext);
        ctx.* = MysqlContext{
            .allocator = allocator,
            .conn = conn,
            .username_z = username_z,
            .database_z = database_z,
        };
        return ctx;
    }

    pub fn deinit(self: *MysqlContext) void {
        self.conn.deinit(self.allocator);
        self.allocator.free(self.username_z);
        self.allocator.free(self.database_z);
        self.allocator.destroy(self);
    }
};

/// MySQL result context
pub const MysqlResultContext = struct {
    allocator: std.mem.Allocator,
    affected: usize = 0,

    pub fn init(allocator: std.mem.Allocator) !*MysqlResultContext {
        const ctx = try allocator.create(MysqlResultContext);
        ctx.* = MysqlResultContext{
            .allocator = allocator,
        };
        return ctx;
    }

    pub fn deinit(self: *MysqlResultContext) void {
        self.allocator.destroy(self);
    }
};

/// VTable for MySQL results
const mysqlResultVTable = ResultVTable{
    .next = mysqlResultNext,
    .columnCount = mysqlResultColumnCount,
    .columnName = mysqlResultColumnName,
    .getValue = mysqlResultGetValue,
    .getValueByName = mysqlResultGetValueByName,
    .affectedRows = mysqlResultAffectedRows,
    .reset = null,
    .deinit = mysqlResultDeinit,
};

fn mysqlResultNext(_: *anyopaque) Error!bool {
    return false;
}

fn mysqlResultColumnCount(_: *anyopaque) usize {
    return 0;
}

fn mysqlResultColumnName(_: *anyopaque, _: usize) ?[]const u8 {
    return null;
}

fn mysqlResultGetValue(_: *anyopaque, _: usize) Error!Value {
    return Error.NoMoreRows;
}

fn mysqlResultGetValueByName(_: *anyopaque, _: []const u8) Error!Value {
    return Error.NotImplemented;
}

fn mysqlResultAffectedRows(ctx: *anyopaque) usize {
    const result_ctx: *MysqlResultContext = @ptrCast(@alignCast(ctx));
    return result_ctx.affected;
}

fn mysqlResultDeinit(ctx: *anyopaque) void {
    const result_ctx: *MysqlResultContext = @ptrCast(@alignCast(ctx));
    result_ctx.deinit();
}

/// VTable for MySQL connections
pub const mysqlConnectionVTable = ConnectionVTable{
    .exec = mysqlExec,
    .query = mysqlQuery,
    .prepare = mysqlPrepare,
    .begin = mysqlBegin,
    .commit = mysqlCommit,
    .rollback = mysqlRollback,
    .close = mysqlClose,
    .lastInsertId = mysqlLastInsertId,
    .affectedRows = mysqlAffectedRows,
    .ping = mysqlPing,
    .lastError = mysqlLastError,
};

fn mysqlExec(ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8, params: []const Value) Error!usize {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    _ = allocator;
    _ = params;

    const result = mysql_ctx.conn.query(sql) catch return Error.ExecutionFailed;
    switch (result) {
        .ok => |ok| {
            mysql_ctx.affected_rows = @intCast(ok.affected_rows);
            mysql_ctx.last_insert_id = @intCast(ok.last_insert_id);
            return mysql_ctx.affected_rows;
        },
        .err => return Error.ExecutionFailed,
    }
}

fn mysqlQuery(ctx: *anyopaque, allocator: std.mem.Allocator, sql: []const u8, params: []const Value) Error!Result {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    _ = params;

    _ = mysql_ctx.conn.query(sql) catch return Error.ExecutionFailed;

    const result_ctx = MysqlResultContext.init(allocator) catch return Error.OutOfMemory;
    return Result.init(@ptrCast(result_ctx), &mysqlResultVTable);
}

fn mysqlPrepare(_: *anyopaque, _: std.mem.Allocator, _: []const u8) Error!Statement {
    return Error.NotImplemented;
}

fn mysqlBegin(ctx: *anyopaque) Error!void {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    _ = mysql_ctx.conn.query("START TRANSACTION") catch return Error.TransactionError;
}

fn mysqlCommit(ctx: *anyopaque) Error!void {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    _ = mysql_ctx.conn.query("COMMIT") catch return Error.TransactionError;
}

fn mysqlRollback(ctx: *anyopaque) Error!void {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    _ = mysql_ctx.conn.query("ROLLBACK") catch return Error.TransactionError;
}

fn mysqlClose(ctx: *anyopaque) void {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    mysql_ctx.deinit();
}

fn mysqlLastInsertId(ctx: *anyopaque) ?i64 {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    return mysql_ctx.last_insert_id;
}

fn mysqlAffectedRows(ctx: *anyopaque) usize {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    return mysql_ctx.affected_rows;
}

fn mysqlPing(ctx: *anyopaque) Error!void {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    mysql_ctx.conn.ping() catch return Error.NotConnected;
}

fn mysqlLastError(ctx: *anyopaque) ?[]const u8 {
    const mysql_ctx: *MysqlContext = @ptrCast(@alignCast(ctx));
    return mysql_ctx.last_error;
}

/// Open a MySQL database connection
pub fn open(allocator: std.mem.Allocator, uri: Uri) Error!Connection {
    const ctx = MysqlContext.init(allocator, uri) catch return Error.ConnectionFailed;

    return Connection{
        .ctx = @ptrCast(ctx),
        .vtable = &mysqlConnectionVTable,
        .allocator = allocator,
        .uri = uri,
    };
}

test "mysql driver interface" {
    // This test only verifies the interface compiles correctly
    // Actual MySQL tests require a running database
    const uri = Uri.parse("mysql://user:pass@localhost:3306/testdb") catch unreachable;
    _ = uri;
}
