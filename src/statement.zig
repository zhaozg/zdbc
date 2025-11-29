//! Prepared statement interface
//!
//! This module provides a unified interface for prepared statements
//! using VTable pattern for zero-cost abstraction.

const std = @import("std");
const Value = @import("value.zig").Value;
const Result = @import("result.zig").Result;
const Error = @import("error.zig").Error;

/// VTable for prepared statement operations
pub const StatementVTable = struct {
    /// Bind a value to a parameter by index (1-based)
    bind: *const fn (ctx: *anyopaque, index: usize, value: Value) Error!void,

    /// Bind multiple values at once
    bindAll: *const fn (ctx: *anyopaque, values: []const Value) Error!void,

    /// Execute the statement (for non-SELECT queries)
    exec: *const fn (ctx: *anyopaque) Error!usize,

    /// Execute the statement and return results (for SELECT queries)
    query: *const fn (ctx: *anyopaque) Error!Result,

    /// Clear all parameter bindings
    clearBindings: *const fn (ctx: *anyopaque) Error!void,

    /// Reset the statement for re-execution
    reset: *const fn (ctx: *anyopaque) Error!void,

    /// Get the number of parameters in the statement
    paramCount: *const fn (ctx: *anyopaque) usize,

    /// Release resources associated with this statement
    deinit: *const fn (ctx: *anyopaque) void,
};

/// Prepared statement with VTable-based polymorphism
pub const Statement = struct {
    /// Pointer to the driver-specific implementation
    ctx: *anyopaque,

    /// VTable containing function pointers
    vtable: *const StatementVTable,

    /// The SQL query for this statement
    sql: []const u8,

    /// Bind a value to a parameter by index (1-based)
    pub fn bind(self: *const Statement, index: usize, value: Value) Error!void {
        return self.vtable.bind(self.ctx, index, value);
    }

    /// Bind multiple values at once
    pub fn bindAll(self: *const Statement, values: []const Value) Error!void {
        return self.vtable.bindAll(self.ctx, values);
    }

    /// Convenience method to bind values from a tuple
    pub fn bindTuple(self: *const Statement, comptime T: type, tuple: T) Error!void {
        const fields = @typeInfo(T).@"struct".fields;
        var values: [fields.len]Value = undefined;
        inline for (fields, 0..) |field, i| {
            const val = @field(tuple, field.name);
            values[i] = @import("value.zig").fromAny(val);
        }
        return self.bindAll(&values);
    }

    /// Execute the statement (for INSERT, UPDATE, DELETE)
    /// Returns the number of affected rows
    pub fn exec(self: *const Statement) Error!usize {
        return self.vtable.exec(self.ctx);
    }

    /// Execute the statement and return results (for SELECT)
    pub fn query(self: *const Statement) Error!Result {
        return self.vtable.query(self.ctx);
    }

    /// Clear all parameter bindings
    pub fn clearBindings(self: *const Statement) Error!void {
        return self.vtable.clearBindings(self.ctx);
    }

    /// Reset the statement for re-execution
    pub fn reset(self: *const Statement) Error!void {
        return self.vtable.reset(self.ctx);
    }

    /// Get the number of parameters in the statement
    pub fn paramCount(self: *const Statement) usize {
        return self.vtable.paramCount(self.ctx);
    }

    /// Release resources
    pub fn deinit(self: *Statement) void {
        self.vtable.deinit(self.ctx);
    }
};

/// Empty statement VTable for testing
pub const emptyStatementVTable = StatementVTable{
    .bind = emptyBind,
    .bindAll = emptyBindAll,
    .exec = emptyExec,
    .query = emptyQuery,
    .clearBindings = emptyClearBindings,
    .reset = emptyReset,
    .paramCount = emptyParamCount,
    .deinit = emptyDeinit,
};

fn emptyBind(_: *anyopaque, _: usize, _: Value) Error!void {}

fn emptyBindAll(_: *anyopaque, _: []const Value) Error!void {}

fn emptyExec(_: *anyopaque) Error!usize {
    return 0;
}

fn emptyQuery(_: *anyopaque) Error!Result {
    return Error.StatementNotPrepared;
}

fn emptyClearBindings(_: *anyopaque) Error!void {}

fn emptyReset(_: *anyopaque) Error!void {}

fn emptyParamCount(_: *anyopaque) usize {
    return 0;
}

fn emptyDeinit(_: *anyopaque) void {}

test "Statement VTable structure" {
    try std.testing.expect(@sizeOf(StatementVTable) > 0);
}
