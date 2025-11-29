//! Result set handling for database queries
//!
//! This module provides a unified interface for iterating over query results
//! using VTable pattern for zero-cost abstraction.

const std = @import("std");
const Value = @import("value.zig").Value;
const Error = @import("error.zig").Error;

/// VTable for result set operations
pub const ResultVTable = struct {
    /// Move to the next row, returns false if no more rows
    next: *const fn (ctx: *anyopaque) Error!bool,

    /// Get the number of columns in the result
    columnCount: *const fn (ctx: *anyopaque) usize,

    /// Get the name of a column by index
    columnName: *const fn (ctx: *anyopaque, index: usize) ?[]const u8,

    /// Get a value from the current row by column index
    getValue: *const fn (ctx: *anyopaque, index: usize) Error!Value,

    /// Get a value from the current row by column name
    getValueByName: *const fn (ctx: *anyopaque, name: []const u8) Error!Value,

    /// Get the number of rows affected (for non-SELECT queries)
    affectedRows: *const fn (ctx: *anyopaque) usize,

    /// Reset the result to iterate again (if supported)
    reset: ?*const fn (ctx: *anyopaque) Error!void,

    /// Release resources associated with this result
    deinit: *const fn (ctx: *anyopaque) void,
};

/// Represents a single row in a result set
pub const Row = struct {
    /// Pointer to the parent Result
    result: *Result,

    /// Get a value by column index
    pub fn get(self: *const Row, index: usize) Error!Value {
        return self.result.vtable.getValue(self.result.ctx, index);
    }

    /// Get a value by column name
    pub fn getByName(self: *const Row, name: []const u8) Error!Value {
        return self.result.vtable.getValueByName(self.result.ctx, name);
    }

    /// Get an integer value by index
    pub fn getInt(self: *const Row, index: usize) Error!?i64 {
        const val = try self.get(index);
        return val.asInt();
    }

    /// Get a float value by index
    pub fn getFloat(self: *const Row, index: usize) Error!?f64 {
        const val = try self.get(index);
        return val.asFloat();
    }

    /// Get a text value by index
    pub fn getText(self: *const Row, index: usize) Error!?[]const u8 {
        const val = try self.get(index);
        return val.asText();
    }

    /// Get a boolean value by index
    pub fn getBool(self: *const Row, index: usize) Error!?bool {
        const val = try self.get(index);
        return val.asBool();
    }

    /// Get a blob value by index
    pub fn getBlob(self: *const Row, index: usize) Error!?[]const u8 {
        const val = try self.get(index);
        return val.asBlob();
    }

    /// Check if a value is null by index
    pub fn isNull(self: *const Row, index: usize) Error!bool {
        const val = try self.get(index);
        return val.isNull();
    }

    /// Get the number of columns
    pub fn columnCount(self: *const Row) usize {
        return self.result.columnCount();
    }

    /// Get a column name by index
    pub fn columnName(self: *const Row, index: usize) ?[]const u8 {
        return self.result.columnName(index);
    }
};

/// Database result set with VTable-based polymorphism
pub const Result = struct {
    /// Pointer to the driver-specific implementation
    ctx: *anyopaque,

    /// VTable containing function pointers
    vtable: *const ResultVTable,

    /// Track if we've started iterating
    started: bool = false,

    pub fn init(ctx: *anyopaque, vtable: *const ResultVTable) Result {
        return Result{
            .ctx = ctx,
            .vtable = vtable,
        };
    }

    /// Get the next row, or null if no more rows
    pub fn next(self: *Result) Error!?Row {
        const has_next = try self.vtable.next(self.ctx);
        if (has_next) {
            self.started = true;
            return Row{ .result = self };
        }
        return null;
    }

    /// Iterate over all rows
    pub fn iterator(self: *Result) RowIterator {
        return RowIterator{ .result = self };
    }

    /// Get the number of columns
    pub fn columnCount(self: *const Result) usize {
        return self.vtable.columnCount(self.ctx);
    }

    /// Get a column name by index
    pub fn columnName(self: *const Result, index: usize) ?[]const u8 {
        return self.vtable.columnName(self.ctx, index);
    }

    /// Get the number of affected rows
    pub fn affectedRows(self: *const Result) usize {
        return self.vtable.affectedRows(self.ctx);
    }

    /// Reset the result to iterate again
    pub fn reset(self: *Result) Error!void {
        if (self.vtable.reset) |reset_fn| {
            try reset_fn(self.ctx);
            self.started = false;
        } else {
            return Error.NotImplemented;
        }
    }

    /// Release resources
    pub fn deinit(self: *Result) void {
        self.vtable.deinit(self.ctx);
    }
};

/// Iterator for rows in a result set
pub const RowIterator = struct {
    result: *Result,

    pub fn next(self: *RowIterator) ?Row {
        return self.result.next() catch null;
    }
};

/// Empty result VTable for testing
pub const emptyResultVTable = ResultVTable{
    .next = emptyNext,
    .columnCount = emptyColumnCount,
    .columnName = emptyColumnName,
    .getValue = emptyGetValue,
    .getValueByName = emptyGetValueByName,
    .affectedRows = emptyAffectedRows,
    .reset = null,
    .deinit = emptyDeinit,
};

fn emptyNext(_: *anyopaque) Error!bool {
    return false;
}

fn emptyColumnCount(_: *anyopaque) usize {
    return 0;
}

fn emptyColumnName(_: *anyopaque, _: usize) ?[]const u8 {
    return null;
}

fn emptyGetValue(_: *anyopaque, _: usize) Error!Value {
    return Error.ColumnOutOfBounds;
}

fn emptyGetValueByName(_: *anyopaque, _: []const u8) Error!Value {
    return Error.ColumnNotFound;
}

fn emptyAffectedRows(_: *anyopaque) usize {
    return 0;
}

fn emptyDeinit(_: *anyopaque) void {}

test "Result VTable structure" {
    try std.testing.expect(@sizeOf(ResultVTable) > 0);
}

test "Row value accessors" {
    var dummy: u8 = 0;
    var result = Result.init(@ptrCast(&dummy), &emptyResultVTable);

    // Test that column count returns 0 for empty result
    try std.testing.expectEqual(@as(usize, 0), result.columnCount());

    // Test that next returns null for empty result
    const row = try result.next();
    try std.testing.expect(row == null);
}
