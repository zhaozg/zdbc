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

**Performance:**
- ~4,000+ records/second in fast mode
- Complete 1 million record insert in ~4-5 minutes

**Build and Run:**
```bash
zig build run-log
```

**Customization:**
Edit the `Config` struct in `log.zig` to adjust:
- `total_records`: Number of records to insert (default: 1,000,000)
- `batch_size`: Records per transaction (default: 1,000)
- `db_path`: Database file location
- `fast_mode`: Enable/disable performance optimizations

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

- Zig 0.15.2 or later
- SQLite3 library (for log example)
