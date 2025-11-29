const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Get dependencies
    const zqlite_dep = b.dependency("zqlite", .{
        .target = target,
        .optimize = optimize,
    });
    const pg_dep = b.dependency("pg", .{
        .target = target,
        .optimize = optimize,
    });
    const myzql_dep = b.dependency("myzql", .{
        .target = target,
        .optimize = optimize,
    });

    // Main library module
    const zdbc_mod = b.addModule("zdbc", .{
        .root_source_file = b.path("src/zdbc.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "zqlite", .module = zqlite_dep.module("zqlite") },
            .{ .name = "pg", .module = pg_dep.module("pg") },
            .{ .name = "myzql", .module = myzql_dep.module("myzql") },
        },
    });
    // Link SQLite C library for zqlite
    zdbc_mod.linkSystemLibrary("sqlite3", .{});

    // Unit tests module
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/zdbc.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{
            .{ .name = "zqlite", .module = zqlite_dep.module("zqlite") },
            .{ .name = "pg", .module = pg_dep.module("pg") },
            .{ .name = "myzql", .module = myzql_dep.module("myzql") },
        },
    });
    test_mod.linkSystemLibrary("sqlite3", .{});

    // Unit tests
    const main_tests = b.addTest(.{
        .name = "zdbc-test",
        .root_module = test_mod,
    });

    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_main_tests.step);

    // Example module
    const example_mod = b.createModule(.{
        .root_source_file = b.path("src/example.zig"),
        .target = target,
        .optimize = optimize,
    });
    example_mod.addImport("zdbc", zdbc_mod);

    // Example executable
    const example = b.addExecutable(.{
        .name = "zdbc-example",
        .root_module = example_mod,
    });
    b.installArtifact(example);

    const run_example = b.addRunArtifact(example);
    run_example.step.dependOn(b.getInstallStep());

    const run_step = b.step("run", "Run the example");
    run_step.dependOn(&run_example.step);
}
