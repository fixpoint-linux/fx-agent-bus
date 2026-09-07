const std = @import("std");

pub fn build(b: *std.Build) void {
    // Force the portable baseline CPU (x86_64 baseline = SSE2 only, no
    // AVX/AVX2/AVX512/BMI) so the binary runs on any host and can never
    // accidentally pick up AVX512. resolveTargetQuery (not
    // standardTargetOptions) is used deliberately so -Dcpu / -Dtarget build
    // flags cannot override it.
    const target = b.resolveTargetQuery(.{ .cpu_model = .baseline });
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "fx-agent-bus",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run fx-agent-bus");
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{ .root_module = exe.root_module });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}
