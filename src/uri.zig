//! URI parser for database connection strings

const std = @import("std");
const Backend = @import("zdbc.zig").Backend;

/// Parsed database URI
pub const Uri = struct {
    backend: Backend,
    host: ?[]const u8 = null,
    port: ?u16 = null,
    database: []const u8,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    options: ?[]const u8 = null,

    /// Parse a URI string into a Uri struct
    /// Supported formats:
    /// - sqlite:///path/to/database.db
    /// - sqlite::memory:
    /// - postgresql://user:password@host:port/database
    /// - postgres://user:password@host:port/database
    /// - mysql://user:password@host:port/database
    pub fn parse(uri_string: []const u8) !Uri {
        // Find scheme separator
        const scheme_end = std.mem.indexOf(u8, uri_string, "://") orelse
            return error.InvalidUri;

        const scheme = uri_string[0..scheme_end];
        const backend = parseBackend(scheme) orelse return error.InvalidUri;

        const rest = uri_string[scheme_end + 3 ..];

        return switch (backend) {
            .sqlite => parseSqliteUri(rest),
            .postgresql, .mysql, .mock => parseNetworkUri(backend, rest),
        };
    }

    fn parseBackend(scheme: []const u8) ?Backend {
        if (std.mem.eql(u8, scheme, "sqlite") or std.mem.eql(u8, scheme, "sqlite3")) {
            return .sqlite;
        } else if (std.mem.eql(u8, scheme, "postgresql") or std.mem.eql(u8, scheme, "postgres")) {
            return .postgresql;
        } else if (std.mem.eql(u8, scheme, "mysql") or std.mem.eql(u8, scheme, "mariadb")) {
            return .mysql;
        } else if (std.mem.eql(u8, scheme, "mock")) {
            return .mock;
        }
        return null;
    }

    fn parseSqliteUri(rest: []const u8) !Uri {
        // Handle :memory: special case
        if (std.mem.eql(u8, rest, ":memory:") or std.mem.eql(u8, rest, "memory") or rest.len == 0) {
            return Uri{
                .backend = .sqlite,
                .database = ":memory:",
            };
        }

        // Check for query parameters
        const query_start = std.mem.indexOf(u8, rest, "?");
        const path = if (query_start) |pos| rest[0..pos] else rest;
        const options = if (query_start) |pos| rest[pos + 1 ..] else null;

        return Uri{
            .backend = .sqlite,
            .database = path,
            .options = options,
        };
    }

    fn parseNetworkUri(backend: Backend, rest: []const u8) !Uri {
        var uri = Uri{
            .backend = backend,
            .database = "",
        };

        var remaining = rest;

        // Check for query parameters
        const query_start = std.mem.indexOf(u8, remaining, "?");
        if (query_start) |pos| {
            uri.options = remaining[pos + 1 ..];
            remaining = remaining[0..pos];
        }

        // Check for userinfo (user:password@)
        const at_pos = std.mem.indexOf(u8, remaining, "@");
        if (at_pos) |pos| {
            const userinfo = remaining[0..pos];
            remaining = remaining[pos + 1 ..];

            // Parse user:password
            const colon_pos = std.mem.indexOf(u8, userinfo, ":");
            if (colon_pos) |cpos| {
                uri.username = userinfo[0..cpos];
                uri.password = userinfo[cpos + 1 ..];
            } else {
                uri.username = userinfo;
            }
        }

        // Parse host:port/database
        const slash_pos = std.mem.indexOf(u8, remaining, "/");
        const host_port = if (slash_pos) |pos| remaining[0..pos] else remaining;
        const database = if (slash_pos) |pos| remaining[pos + 1 ..] else "";

        // Parse host:port
        const colon_pos = std.mem.lastIndexOf(u8, host_port, ":");
        if (colon_pos) |pos| {
            uri.host = host_port[0..pos];
            uri.port = std.fmt.parseInt(u16, host_port[pos + 1 ..], 10) catch null;
        } else {
            uri.host = if (host_port.len > 0) host_port else null;
        }

        // Set default ports
        if (uri.port == null) {
            uri.port = switch (backend) {
                .postgresql => 5432,
                .mysql => 3306,
                .sqlite, .mock => null,
            };
        }

        uri.database = database;

        return uri;
    }

    /// Get the default port for the backend
    pub fn defaultPort(backend: Backend) ?u16 {
        return switch (backend) {
            .sqlite, .mock => null,
            .postgresql => 5432,
            .mysql => 3306,
        };
    }
};

test "parse sqlite memory URI" {
    const uri = try Uri.parse("sqlite://:memory:");
    try std.testing.expectEqual(Backend.sqlite, uri.backend);
    try std.testing.expectEqualStrings(":memory:", uri.database);
}

test "parse sqlite file URI" {
    const uri = try Uri.parse("sqlite:///path/to/db.sqlite");
    try std.testing.expectEqual(Backend.sqlite, uri.backend);
    try std.testing.expectEqualStrings("/path/to/db.sqlite", uri.database);
}

test "parse postgresql URI" {
    const uri = try Uri.parse("postgresql://user:pass@localhost:5432/mydb");
    try std.testing.expectEqual(Backend.postgresql, uri.backend);
    try std.testing.expectEqualStrings("localhost", uri.host.?);
    try std.testing.expectEqual(@as(u16, 5432), uri.port.?);
    try std.testing.expectEqualStrings("mydb", uri.database);
    try std.testing.expectEqualStrings("user", uri.username.?);
    try std.testing.expectEqualStrings("pass", uri.password.?);
}

test "parse mysql URI" {
    const uri = try Uri.parse("mysql://root:secret@db.example.com/testdb");
    try std.testing.expectEqual(Backend.mysql, uri.backend);
    try std.testing.expectEqualStrings("db.example.com", uri.host.?);
    try std.testing.expectEqual(@as(u16, 3306), uri.port.?);
    try std.testing.expectEqualStrings("testdb", uri.database);
    try std.testing.expectEqualStrings("root", uri.username.?);
    try std.testing.expectEqualStrings("secret", uri.password.?);
}

test "parse URI with options" {
    const uri = try Uri.parse("postgresql://user@host/db?ssl=true&timeout=30");
    try std.testing.expectEqual(Backend.postgresql, uri.backend);
    try std.testing.expectEqualStrings("ssl=true&timeout=30", uri.options.?);
}
