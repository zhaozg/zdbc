# ZDBC - Zig Database Connector

A high-performance, type-safe database abstraction layer for Zig using the VTable pattern.

## Features

- **VTable Pattern**: Zero-cost abstraction using Zig's compile-time features
- **Multiple Backends**: Support for SQLite, PostgreSQL, and MySQL
- **Unified Interface**: Consistent API across all database backends
- **Connection Pooling**: Built-in thread-safe connection pool
- **URI-based Connections**: Easy connection string parsing
- **Type Safety**: Compile-time type checking for database operations
- **SQL Injection Prevention**: Parameterized queries only

## Supported Databases

| Database   | Library                                                    | Status       |
|------------|-----------------------------------------------------------|--------------|
| SQLite     | [zqlite.zig](https://github.com/karlseguin/zqlite.zig)   | Planned      |
| PostgreSQL | [pg.zig](https://github.com/karlseguin/pg.zig)           | Planned      |
| MySQL      | [myzql](https://github.com/speed2exe/myzql)              | Planned      |
| Mock       | Built-in                                                  | ✅ Available |

## Installation

Add ZDBC to your `build.zig.zon`:

```zig
.dependencies = .{
    .zdbc = .{
        .url = "https://github.com/zhaozg/zdbc/archive/refs/heads/main.tar.gz",
        // Add hash after first build
    },
},
```

In your `build.zig`:

```zig
const zdbc = b.dependency("zdbc", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zdbc", zdbc.module("zdbc"));
```

## Quick Start

```zig
const std = @import("std");
const zdbc = @import("zdbc");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open a connection
    var conn = try zdbc.open(allocator, "sqlite:///path/to/db.sqlite");
    defer conn.close();

    // Execute DDL
    _ = try conn.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", &.{});

    // Insert with parameters
    _ = try conn.exec(
        "INSERT INTO users (name) VALUES (?)",
        &.{zdbc.Value.initText("John")},
    );

    // Query rows
    var result = try conn.query("SELECT * FROM users", &.{});
    defer result.deinit();

    while (try result.next()) |row| {
        const id = (try row.getInt(0)).?;
        const name = (try row.getText(1)).?;
        std.debug.print("User: {} - {s}\n", .{id, name});
    }
}
```

## URI Formats

ZDBC supports URI-based connection strings:

```
# SQLite
sqlite:///path/to/database.db
sqlite://:memory:

# PostgreSQL
postgresql://user:password@host:port/database
postgres://user:password@host:port/database?options

# MySQL
mysql://user:password@host:port/database
mariadb://user:password@host:port/database
```

## API Reference

### Connection

```zig
// Create a connection
var conn = try zdbc.open(allocator, "sqlite:///test.db");
defer conn.close();

// Execute queries (INSERT, UPDATE, DELETE)
const affected = try conn.exec("INSERT INTO users (name) VALUES (?)", &.{
    zdbc.Value.initText("John"),
});

// Query rows (SELECT)
var result = try conn.query("SELECT * FROM users", &.{});
defer result.deinit();

// Single row query
const row = try conn.row("SELECT * FROM users WHERE id = ?", &.{
    zdbc.Value.initInt(1),
});

// Transactions
try conn.begin();
errdefer conn.rollback() catch {};
// ... operations ...
try conn.commit();
```

### Value Types

```zig
// Create values for parameter binding
const null_val = zdbc.Value.initNull();
const bool_val = zdbc.Value.initBool(true);
const int_val = zdbc.Value.initInt(42);
const float_val = zdbc.Value.initFloat(3.14);
const text_val = zdbc.Value.initText("hello");
const blob_val = zdbc.Value.initBlob(&[_]u8{0x01, 0x02, 0x03});

// Or use the convenience function
const val = zdbc.fromAny("hello"); // Automatically detects type
```

### Result Iteration

```zig
var result = try conn.query("SELECT id, name, age FROM users", &.{});
defer result.deinit();

// Get column info
const col_count = result.columnCount();
const col_name = result.columnName(0); // "id"

// Iterate rows
while (try result.next()) |row| {
    const id = try row.getInt(0);
    const name = try row.getText(1);
    const age = try row.getInt(2);
}
```

### Connection Pool

```zig
const pool = try zdbc.Pool.init(allocator, "postgresql://localhost/mydb", .{
    .min_size = 5,
    .max_size = 20,
    .acquire_timeout_ms = 30000,
});
defer pool.deinit();

// Acquire connection from pool
const pooled = try pool.acquire();
defer pooled.release();

const conn = pooled.connection();
_ = try conn.exec("SELECT 1", &.{});
```

## Architecture

ZDBC uses the VTable pattern for polymorphism:

```
┌─────────────────┐
│    Connection   │ ──── User Interface
├─────────────────┤
│     VTable      │ ──── Function Pointers
├─────────────────┤
│  ctx: *anyopaque│ ──── Driver-specific Context
└─────────────────┘
         │
         ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  SQLite Driver  │ │   PG Driver     │ │  MySQL Driver   │
│  (zqlite.zig)   │ │   (pg.zig)      │ │   (myzql)       │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

This pattern provides:
- **Zero-cost abstraction**: Direct function calls through pointers
- **Type safety**: Compile-time interface verification
- **Extensibility**: Easy to add new database drivers

## Building

```bash
# Build the library
zig build

# Run tests
zig build test

# Run the example
zig build run

# Format code
zig fmt src
```

## Requirements

- Zig 0.15.x or later
- For SQLite: `libsqlite3-dev` (system library)
- For PostgreSQL: Network access to PostgreSQL server
- For MySQL: Network access to MySQL/MariaDB server

## Testing

Run the test suite:

```bash
zig build test
```

For testing without a real database, use the mock driver:

```zig
var conn = try zdbc.openMock(allocator);
defer conn.close();
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `zig fmt src` and `zig build test`
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- [zqlite.zig](https://github.com/karlseguin/zqlite.zig) - SQLite wrapper
- [pg.zig](https://github.com/karlseguin/pg.zig) - PostgreSQL driver
- [myzql](https://github.com/speed2exe/myzql) - MySQL/MariaDB driver
