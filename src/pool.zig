//! Connection pool for database connections
//!
//! Provides a thread-safe pool of database connections for efficient
//! connection reuse and management.

const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Error = @import("error.zig").Error;
const Uri = @import("uri.zig").Uri;
const zdbc = @import("zdbc.zig");

/// Configuration for the connection pool
pub const PoolConfig = struct {
    /// Minimum number of connections to maintain
    min_size: usize = 1,

    /// Maximum number of connections allowed
    max_size: usize = 10,

    /// Maximum time to wait for a connection (in milliseconds)
    acquire_timeout_ms: u64 = 30000,

    /// Time before an idle connection is closed (in milliseconds)
    idle_timeout_ms: u64 = 300000,

    /// Whether to validate connections before returning them
    validate_on_acquire: bool = true,

    /// Callback to initialize each connection
    on_connection: ?*const fn (*Connection) Error!void = null,
};

/// Pooled connection wrapper
pub const PooledConnection = struct {
    /// The underlying connection
    conn: Connection,

    /// Reference to the parent pool
    pool: *Pool,

    /// Whether this connection is currently in use
    in_use: bool = false,

    /// Last time this connection was used
    last_used: i64 = 0,

    /// Release this connection back to the pool
    pub fn release(self: *PooledConnection) void {
        self.pool.releaseConnection(self);
    }

    /// Access the underlying connection
    pub fn connection(self: *PooledConnection) *Connection {
        return &self.conn;
    }
};

/// Thread-safe connection pool
pub const Pool = struct {
    /// Allocator for pool operations
    allocator: std.mem.Allocator,

    /// Pool configuration
    config: PoolConfig,

    /// URI for creating new connections
    uri: Uri,

    /// All connections in the pool
    connections: std.ArrayList(PooledConnection),

    /// Available connection indices
    available: std.ArrayList(usize),

    /// Mutex for thread safety
    mutex: std.Thread.Mutex = .{},

    /// Condition variable for waiting on connections
    condition: std.Thread.Condition = .{},

    /// Whether the pool has been closed
    closed: bool = false,

    /// Initialize a new connection pool
    pub fn init(allocator: std.mem.Allocator, uri_string: []const u8, config: PoolConfig) Error!*Pool {
        const uri = Uri.parse(uri_string) catch return Error.InvalidUri;
        return initWithUri(allocator, uri, config);
    }

    /// Initialize a new connection pool with a parsed URI
    pub fn initWithUri(allocator: std.mem.Allocator, uri: Uri, config: PoolConfig) Error!*Pool {
        const pool = allocator.create(Pool) catch return Error.OutOfMemory;
        pool.* = Pool{
            .allocator = allocator,
            .config = config,
            .uri = uri,
            .connections = std.ArrayList(PooledConnection).init(allocator),
            .available = std.ArrayList(usize).init(allocator),
        };

        // Create minimum number of connections
        var i: usize = 0;
        while (i < config.min_size) : (i += 1) {
            pool.createConnection() catch |err| {
                pool.deinit();
                return err;
            };
        }

        return pool;
    }

    /// Create a new connection and add it to the pool
    fn createConnection(self: *Pool) Error!void {
        var conn = try zdbc.openWithUri(self.allocator, self.uri);

        // Call initialization callback if provided
        if (self.config.on_connection) |callback| {
            try callback(&conn);
        }

        const index = self.connections.items.len;
        self.connections.append(PooledConnection{
            .conn = conn,
            .pool = self,
            .in_use = false,
            .last_used = std.time.milliTimestamp(),
        }) catch return Error.OutOfMemory;

        self.available.append(index) catch return Error.OutOfMemory;
    }

    /// Acquire a connection from the pool
    pub fn acquire(self: *Pool) Error!*PooledConnection {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.closed) {
            return Error.PoolExhausted;
        }

        // Try to get an available connection
        while (self.available.items.len == 0) {
            // Try to create a new connection if below max size
            if (self.connections.items.len < self.config.max_size) {
                // Create connection while holding the lock to avoid race conditions
                self.createConnection() catch {};
                continue;
            }

            // Wait for a connection to become available
            const timeout_ns = self.config.acquire_timeout_ms * 1_000_000;
            self.condition.timedWait(&self.mutex, timeout_ns) catch {
                return Error.Timeout;
            };

            if (self.closed) {
                return Error.PoolExhausted;
            }
        }

        // Get an available connection
        const index = self.available.pop();
        var pooled_conn = &self.connections.items[index];
        pooled_conn.in_use = true;
        pooled_conn.last_used = std.time.milliTimestamp();

        // Validate connection if configured
        if (self.config.validate_on_acquire) {
            pooled_conn.conn.ping() catch {
                // Connection is invalid, try to reconnect
                pooled_conn.conn.close();
                pooled_conn.conn = zdbc.openWithUri(self.allocator, self.uri) catch {
                    // Failed to reconnect, mark as not in use and try again
                    pooled_conn.in_use = false;
                    return Error.ConnectionFailed;
                };
            };
        }

        return pooled_conn;
    }

    /// Release a connection back to the pool
    pub fn releaseConnection(self: *Pool, pooled_conn: *PooledConnection) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.closed) {
            pooled_conn.conn.close();
            return;
        }

        pooled_conn.in_use = false;
        pooled_conn.last_used = std.time.milliTimestamp();

        // Find the index of this connection
        for (self.connections.items, 0..) |*conn, i| {
            if (conn == pooled_conn) {
                self.available.append(i) catch |err| {
                    std.log.err("Failed to return connection to pool: {}", .{err});
                };
                break;
            }
        }

        self.condition.signal();
    }

    /// Release a connection (alternative to pooled_conn.release())
    pub fn release(self: *Pool, pooled_conn: *PooledConnection) void {
        self.releaseConnection(pooled_conn);
    }

    /// Get the number of connections in the pool
    pub fn size(self: *Pool) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.connections.items.len;
    }

    /// Get the number of available connections
    pub fn availableCount(self: *Pool) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.available.items.len;
    }

    /// Close all connections and release resources
    pub fn deinit(self: *Pool) void {
        self.mutex.lock();
        self.closed = true;
        self.mutex.unlock();

        // Wake up any waiting threads
        self.condition.broadcast();

        // Close all connections
        for (self.connections.items) |*pooled_conn| {
            pooled_conn.conn.close();
        }

        self.connections.deinit();
        self.available.deinit();
        self.allocator.destroy(self);
    }
};

test "PoolConfig defaults" {
    const config = PoolConfig{};
    try std.testing.expectEqual(@as(usize, 1), config.min_size);
    try std.testing.expectEqual(@as(usize, 10), config.max_size);
    try std.testing.expect(config.validate_on_acquire);
}
