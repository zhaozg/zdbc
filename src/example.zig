//! ZDBC Example Usage
//!
//! This example demonstrates the basic usage of ZDBC for database operations.

const std = @import("std");
const zdbc = @import("zdbc");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Example 1: Using mock driver for testing
    std.debug.print("=== ZDBC Example ===\n\n", .{});

    // Open a mock connection (for testing without a real database)
    var conn = try zdbc.openMock(allocator);
    defer conn.close();

    std.debug.print("1. Connected to mock database\n", .{});

    // Ping the database
    try conn.ping();
    std.debug.print("2. Ping successful\n", .{});

    // Execute a statement (mock just returns 1 affected row)
    const affected = try conn.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", &.{});
    std.debug.print("3. Created table, affected rows: {}\n", .{affected});

    // Insert with parameters
    const inserted = try conn.exec(
        "INSERT INTO users (name) VALUES (?)",
        &.{zdbc.Value.initText("John")},
    );
    std.debug.print("4. Inserted row, affected rows: {}\n", .{inserted});

    // Get last insert ID
    if (conn.lastInsertId()) |id| {
        std.debug.print("5. Last insert ID: {}\n", .{id});
    }

    // Transaction example
    try conn.begin();
    std.debug.print("6. Transaction started\n", .{});

    _ = try conn.exec("INSERT INTO users (name) VALUES (?)", &.{zdbc.Value.initText("Jane")});

    try conn.commit();
    std.debug.print("7. Transaction committed\n", .{});

    // Query example (mock returns empty result)
    var result = try conn.query("SELECT * FROM users", &.{});
    defer result.deinit();

    std.debug.print("8. Query executed, columns: {}\n", .{result.columnCount()});

    // Iterate over rows
    while (try result.next()) |row| {
        std.debug.print("   Row: column count = {}\n", .{row.columnCount()});
    }

    std.debug.print("\n=== Example Complete ===\n", .{});

    // Example of URI parsing
    std.debug.print("\n=== URI Parsing Examples ===\n", .{});

    const uris = [_][]const u8{
        "sqlite:///path/to/db.sqlite",
        "sqlite://:memory:",
        "postgresql://user:pass@localhost:5432/mydb",
        "mysql://root:secret@db.example.com:3306/testdb",
    };

    for (uris) |uri_str| {
        if (zdbc.Uri.parse(uri_str)) |uri| {
            std.debug.print("URI: {s}\n", .{uri_str});
            std.debug.print("  Backend: {s}\n", .{@tagName(uri.backend)});
            std.debug.print("  Database: {s}\n", .{uri.database});
            if (uri.host) |host| std.debug.print("  Host: {s}\n", .{host});
            if (uri.port) |port| std.debug.print("  Port: {}\n", .{port});
            if (uri.username) |user| std.debug.print("  Username: {s}\n", .{user});
            std.debug.print("\n", .{});
        } else |_| {
            std.debug.print("Failed to parse URI: {s}\n", .{uri_str});
        }
    }
}
