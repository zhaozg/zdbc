# ZDBC Examples

This directory contains example programs demonstrating various features and use cases of ZDBC.

## Examples

### log.zig - High-Performance Logging System

A demonstration of high-performance bulk insert operations using SQLite, capable of inserting 1 million records efficiently.

**Features:**
- Transaction batching (1000 records per transaction)
- Multi-value INSERT statements
- SQLite performance optimizations (WAL mode, cache tuning)
- Deferred index creation
- Performance monitoring and metrics
- Performance comparison mode (with/without transactions, prepared statements)

**Table Schema:**
- `id`: Auto-increment primary key
- `timestamp`: Unix timestamp in milliseconds
- `who`: User identifier or system name
- `operation`: Operation type/action performed
- `target`: Target resource or entity (NEW)
- `result`: Operation result status
- `remark`: Additional notes or details

**Performance:**
- ~2,000-4,000 records/second in fast mode
- Complete 1 million record insert in ~4-8 minutes
- Multi-value INSERTs are 13% faster than individual INSERTs

**Build and Run:**
```bash
# Standard benchmark (1M records)
zig build run-log

# Performance comparison mode (10K records)
./zig-out/bin/log-example --compare
```

**Comparison Results:**
- Multi-value INSERT with Transaction: ~4,184 records/sec
- Individual INSERTs with Transaction: ~3,635 records/sec (13% slower)
- Multi-value INSERT without Transaction: ~4,188 records/sec

**Customization:**
Edit the `Config` struct in `log.zig` to adjust:
- `total_records`: Number of records to insert (default: 1,000,000)
- `batch_size`: Records per transaction (default: 1,000)
- `db_path`: Database file location
- `fast_mode`: Enable/disable performance optimizations

**Compatibility:**
- Supports Zig 0.14.x and 0.15.x
- Uses compatibility layer for ArrayList API differences

## Running Examples

To build all examples:
```bash
zig build
```

To run the basic example:
```bash
zig build run
```

To run the log example:
```bash
zig build run-log
```

## Requirements

- Zig 0.14.x or 0.15.x
- SQLite3 library (for log example)
