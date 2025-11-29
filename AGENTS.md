# AGENT.md

ZDBC - Zig Database Connector

本项目目标为实现一个高性能、可扩展、通用SQL数据库支持的连接器。

## 功能

- 支持基于 URI 完成数据库连接
- 支持 多种 SQL 数据库（目前为 MySQL, PostgreSQL, SQLite 三种）
- 具有 连接池 功能，提升数据库连接效率
- 具有主流 SQL 操作的封装，简化数据库操作, 目标是通用性，能满足应用系统对数据库常用操作的需求

## 要求

1. 如有成熟封装的库, 可以作为模块使用。
1. pg, mysql, sqlite 三种数据库必须支持。
1. 优先使用纯 zig 实现。
1. 对于 sqlite3, 可以使用 C 语言的 sqlite3 库进行绑定。
1. 注意接口的安全性处理， 防治 SQL 注入等安全问题。
1. 模块化设计， 通用性强，易于扩展。
1. 不同数据库的接口尽量保持一致， 方便用户切换数据库。
1. 不同数据库具体的实现在各自独立的文件中， 方便后续维护和扩展。

## Build/Lint/Test Commands

- `zig build` - Compile zig source code to zig-out/bin/
- `zig build test` - Run All units test
- `zig fmt --check src` - Run code style check

## requirements

- Must build and test pass with zig 0.15.x, download URL in https://ziglang.org/download/index.json
- All CI must pass.
- Update benchmark results with ReleaseFast mode in README.md if applicable.
- Push PR after `zig fmt src`
