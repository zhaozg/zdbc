const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Main library module
    const zdbc_mod = b.addModule("zdbc", .{
        .root_source_file = b.path("src/zdbc.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Link system libraries for SQLite
    zdbc_mod.link_system_library("sqlite3", .{ .needed = true });
    zdbc_mod.link_libc();

    // Library artifact
    const lib = b.addStaticLibrary(.{
        .name = "zdbc",
        .root_source_file = b.path("src/zdbc.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.linkSystemLibrary("sqlite3");
    lib.linkLibC();
    b.installArtifact(lib);

    // Unit tests
    const main_tests = b.addTest(.{
        .root_source_file = b.path("src/zdbc.zig"),
        .target = target,
        .optimize = optimize,
    });
    main_tests.linkSystemLibrary("sqlite3");
    main_tests.linkLibC();

    const run_main_tests = b.addRunArtifact(main_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_main_tests.step);

    // Example executable
    const example = b.addExecutable(.{
        .name = "zdbc-example",
        .root_source_file = b.path("src/example.zig"),
        .target = target,
        .optimize = optimize,
    });
    example.root_module.addImport("zdbc", zdbc_mod);
    example.linkSystemLibrary("sqlite3");
    example.linkLibC();
    b.installArtifact(example);

    const run_example = b.addRunArtifact(example);
    run_example.step.dependOn(b.getInstallStep());

    const run_step = b.step("run", "Run the example");
    run_step.dependOn(&run_example.step);
}
