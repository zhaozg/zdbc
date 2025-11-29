//! Value types for database parameters and results

const std = @import("std");

/// Database value union representing all supported types
pub const Value = union(enum) {
    null: void,
    boolean: bool,
    int: i64,
    uint: u64,
    float: f64,
    text: []const u8,
    blob: []const u8,

    /// Create a null value
    pub fn initNull() Value {
        return .{ .null = {} };
    }

    /// Create a boolean value
    pub fn initBool(val: bool) Value {
        return .{ .boolean = val };
    }

    /// Create an integer value
    pub fn initInt(val: anytype) Value {
        const T = @TypeOf(val);
        const info = @typeInfo(T);
        return switch (info) {
            .int => |i| if (i.signedness == .signed)
                .{ .int = @intCast(val) }
            else
                .{ .uint = @intCast(val) },
            .comptime_int => .{ .int = val },
            else => @compileError("Expected integer type"),
        };
    }

    /// Create a float value
    pub fn initFloat(val: anytype) Value {
        return .{ .float = @floatCast(val) };
    }

    /// Create a text value
    pub fn initText(val: []const u8) Value {
        return .{ .text = val };
    }

    /// Create a blob value
    pub fn initBlob(val: []const u8) Value {
        return .{ .blob = val };
    }

    /// Check if this value is null
    pub fn isNull(self: Value) bool {
        return self == .null;
    }

    /// Get as boolean, returns null if not a boolean
    pub fn asBool(self: Value) ?bool {
        return switch (self) {
            .boolean => |v| v,
            .int => |v| v != 0,
            .uint => |v| v != 0,
            else => null,
        };
    }

    /// Get as signed integer, returns null if not an integer
    pub fn asInt(self: Value) ?i64 {
        return switch (self) {
            .int => |v| v,
            .uint => |v| if (v <= std.math.maxInt(i64)) @intCast(v) else null,
            .boolean => |v| if (v) @as(i64, 1) else @as(i64, 0),
            else => null,
        };
    }

    /// Get as unsigned integer, returns null if not an integer
    pub fn asUint(self: Value) ?u64 {
        return switch (self) {
            .uint => |v| v,
            .int => |v| if (v >= 0) @intCast(v) else null,
            .boolean => |v| if (v) @as(u64, 1) else @as(u64, 0),
            else => null,
        };
    }

    /// Get as float, returns null if not a number
    pub fn asFloat(self: Value) ?f64 {
        return switch (self) {
            .float => |v| v,
            .int => |v| @floatFromInt(v),
            .uint => |v| @floatFromInt(v),
            else => null,
        };
    }

    /// Get as text, returns null if not text
    pub fn asText(self: Value) ?[]const u8 {
        return switch (self) {
            .text => |v| v,
            else => null,
        };
    }

    /// Get as blob, returns null if not blob
    pub fn asBlob(self: Value) ?[]const u8 {
        return switch (self) {
            .blob => |v| v,
            .text => |v| v,
            else => null,
        };
    }

    /// Format for printing
    pub fn format(self: Value, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        switch (self) {
            .null => try writer.writeAll("NULL"),
            .boolean => |v| try writer.print("{}", .{v}),
            .int => |v| try writer.print("{}", .{v}),
            .uint => |v| try writer.print("{}", .{v}),
            .float => |v| try writer.print("{d}", .{v}),
            .text => |v| try writer.print("'{s}'", .{v}),
            .blob => |v| try writer.print("<blob:{d} bytes>", .{v.len}),
        }
    }
};

/// Convert any Zig value to a database Value
pub fn fromAny(val: anytype) Value {
    const T = @TypeOf(val);
    const info = @typeInfo(T);

    if (T == Value) {
        return val;
    }

    if (comptime T == @TypeOf(null)) {
        return Value.initNull();
    }

    return switch (info) {
        .null => Value.initNull(),
        .bool => Value.initBool(val),
        .int, .comptime_int => Value.initInt(val),
        .float, .comptime_float => Value.initFloat(val),
        .pointer => |ptr| blk: {
            if (ptr.size == .slice and ptr.child == u8) {
                break :blk Value.initText(val);
            } else if (ptr.size == .one and @typeInfo(ptr.child) == .array) {
                const child_info = @typeInfo(ptr.child);
                if (child_info.array.child == u8) {
                    break :blk Value.initText(val);
                }
            }
            @compileError("Unsupported pointer type");
        },
        .array => |arr| if (arr.child == u8) Value.initText(&val) else @compileError("Unsupported array type"),
        .optional => if (val) |v| fromAny(v) else Value.initNull(),
        else => @compileError("Unsupported type: " ++ @typeName(T)),
    };
}

test "Value creation and access" {
    const null_val = Value.initNull();
    try std.testing.expect(null_val.isNull());

    const bool_val = Value.initBool(true);
    try std.testing.expectEqual(true, bool_val.asBool());

    const int_val = Value.initInt(@as(i64, 42));
    try std.testing.expectEqual(@as(i64, 42), int_val.asInt());

    const float_val = Value.initFloat(@as(f64, 3.14));
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), float_val.asFloat().?, 0.001);

    const text_val = Value.initText("hello");
    try std.testing.expectEqualStrings("hello", text_val.asText().?);
}

test "fromAny conversion" {
    const null_val = fromAny(null);
    try std.testing.expect(null_val.isNull());

    const bool_val = fromAny(true);
    try std.testing.expectEqual(true, bool_val.asBool());

    const int_val = fromAny(@as(i32, 42));
    try std.testing.expectEqual(@as(i64, 42), int_val.asInt());

    const str_val = fromAny("hello");
    try std.testing.expectEqualStrings("hello", str_val.asText().?);
}
