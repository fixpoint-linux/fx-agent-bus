// fx-agent-bus — a CLI real-time message bus for hax agents.
//
// One daemon (autostarted on first use) holds per-channel FIFO queues over an
// AF_UNIX stream socket; clients connect, exchange one frame, and exit. See
// src/protocol.zig for the wire format and src/daemon.zig for the server.

const std = @import("std");
const protocol = @import("protocol.zig");
const daemon = @import("daemon.zig");
const Allocator = std.mem.Allocator;

// Stale-socket recovery legitimately hits ECONNREFUSED (mapped to
// error.Unexpected); keep std from dumping a stack trace for an expected path.
pub const std_options: std.Options = .{ .unexpected_error_tracing = false };

const usage_text =
    \\fx-agent-bus — real-time message bus for hax agents
    \\
    \\usage: fx-agent-bus <command> [args]
    \\
    \\  daemon                     run the bus daemon in the foreground
    \\  start                      autostart the daemon, print its pid
    \\  send <ch> <body...>        append a message to channel <ch>
    \\  send --multi <ch> <body...>
    \\                             wake every parked waiter (one copy each)
    \\                             and queue a copy for later consumers
    \\                             (a plain send fans out the same way when
    \\                             more than one waiter is already parked)
    \\  recv <ch> [--timeout SEC]  pop the oldest message (waits if empty)
    \\  recv <ch> --from ID [--timeout SEC]
    \\                             catch up: replay oldest message newer than
    \\                             ID, else wait for the next one
    \\  recv --any <ch>... [--timeout SEC]
    \\                             pop from the first of several channels
    \\  poll <ch>                  report queue depth without consuming
    \\  history <ch> [--after ID]  print retained messages after id (catch-up)
    \\  list                       list channels (queued/parked counts)
    \\  close <ch>                 drop a channel and its queued messages
    \\  who                        print daemon pid, uptime and socket path
    \\  shutdown                   stop the daemon (queues are not persisted)
    \\  ping                       liveness check
    \\
    \\env: FX_AGENT_BUS_SOCK overrides the default ~/.config/hax/bus.sock
    \\     FX_AGENT_BUS_RECV_TIMEOUT default recv wait when --timeout omitted (s, default 60)
    \\     FX_AGENT_BUS_MAX_WAIT      daemon cap on any parked recv (s, default 300; 0 disables)
    \\
;

fn fail(comptime fmt: []const u8, args: anytype) noreturn {
    std.debug.print("fx-agent-bus: " ++ fmt ++ "\n", args);
    std.process.exit(1);
}

fn usageExit() noreturn {
    std.debug.print("{s}", .{usage_text});
    std.process.exit(2);
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const arena = init.arena.allocator();
    const args = try init.minimal.args.toSlice(arena);

    if (args.len < 2) usageExit();

    const sock_path = sockPath(arena, init.environ_map) catch
        fail("cannot determine socket path (set FX_AGENT_BUS_SOCK)", .{});

    const cmd = args[1];
    if (std.mem.eql(u8, cmd, "daemon")) {
        daemon.run(io, init.gpa, sock_path, maxWaitMs(arena, init.environ_map)) catch |e|
            fail("daemon failed: {s}", .{@errorName(e)});
        return;
    }
    if (std.mem.eql(u8, cmd, "ping")) {
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeSimple(arena, "ping"), null);
        exitOnErr(resp);
        printOut(io, arena, "pong\n", .{});
        return;
    }
    if (std.mem.eql(u8, cmd, "who")) {
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeSimple(arena, "who"), null);
        exitOnErr(resp);
        printOut(io, arena, "pid {d} uptime_ms {d} sock {s}\n", .{ resp.pid.?, resp.uptime_ms.?, resp.sock.? });
        return;
    }
    if (std.mem.eql(u8, cmd, "start")) {
        daemon.ensureDaemon(io, init.gpa, sock_path) catch |e|
            fail("daemon failed to start ({s})", .{@errorName(e)});
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeSimple(arena, "who"), null);
        exitOnErr(resp);
        printOut(io, arena, "pid {d}\n", .{resp.pid.?});
        return;
    }
    if (std.mem.eql(u8, cmd, "shutdown")) {
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeSimple(arena, "shutdown"), null);
        exitOnErr(resp);
        printOut(io, arena, "bye\n", .{});
        return;
    }
    if (std.mem.eql(u8, cmd, "list")) {
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeSimple(arena, "list"), null);
        exitOnErr(resp);
        var out: std.ArrayList(u8) = .empty;
        for (resp.channels orelse &.{}) |ci| {
            out.print(arena, "{s} queued={d} parked={d}\n", .{ ci.ch, ci.queued, ci.parked }) catch {};
        }
        printOut(io, arena, "{s}", .{out.items});
        return;
    }
    if (std.mem.eql(u8, cmd, "send")) {
        var multi = false;
        var rest = args[2..];
        if (rest.len > 0 and std.mem.eql(u8, rest[0], "--multi")) {
            multi = true;
            rest = rest[1..];
        }
        if (rest.len < 2) usageExit();
        const ch = rest[0];
        // `send <ch> -` reads the body from stdin (argv is capped at ~128KiB
        // per element, so large payloads must go this way).
        const body: []const u8 = if (rest.len == 2 and std.mem.eql(u8, rest[1], "-")) readStdin(io, arena) else std.mem.join(arena, " ", rest[1..]) catch
            fail("out of memory", .{});
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeSend(arena, ch, body, multi), null);
        exitOnErr(resp);
        printOut(io, arena, "id {d} delivered {d}\n", .{ resp.id.?, resp.woken orelse 0 });
        return;
    }
    if (std.mem.eql(u8, cmd, "recv")) {
        return cmdRecv(io, arena, sock_path, init.environ_map, args[2..]);
    }
    if (std.mem.eql(u8, cmd, "close")) {
        if (args.len != 3) usageExit();
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeClose(arena, args[2]), null);
        exitOnErr(resp);
        printOut(io, arena, "closed {s}\n", .{args[2]});
        return;
    }
    if (std.mem.eql(u8, cmd, "poll")) {
        if (args.len != 3) usageExit();
        const resp = roundtrip(io, arena, sock_path, try protocol.encodePoll(arena, args[2]), null);
        exitOnErr(resp);
        if (resp.empty orelse false) {
            printOut(io, arena, "empty\n", .{});
        } else {
            printOut(io, arena, "{d} queued\n", .{resp.count.?});
        }
        return;
    }
    if (std.mem.eql(u8, cmd, "history")) {
        var after: ?u64 = null;
        const rest = args[2..];
        // history <ch> [--after ID]
        if (rest.len >= 2 and std.mem.eql(u8, rest[1], "--after")) {
            if (rest.len < 3) usageExit();
            after = std.fmt.parseUnsigned(u64, rest[2], 10) catch
                fail("bad --after value '{s}'", .{rest[2]});
        } else if (rest.len >= 2) {
            usageExit();
        }
        if (rest.len == 0) usageExit();
        const ch = rest[0];
        const resp = roundtrip(io, arena, sock_path, try protocol.encodeHistory(arena, ch, after), null);
        exitOnErr(resp);
        for (resp.history orelse &.{}) |m| {
            printOut(io, arena, "id {d} ts {d} {s}\n", .{ m.id, m.ts, m.body });
        }
        return;
    }

    usageExit();
}

fn cmdRecv(io: std.Io, arena: Allocator, sock_path: []const u8, env: *std.process.Environ.Map, rest: []const []const u8) !void {
    var timeout_s: ?f64 = null;
    var any = false;
    var after: ?u64 = null;
    var chans: std.ArrayList([]const u8) = .empty;
    var i: usize = 0;
    while (i < rest.len) : (i += 1) {
        const a = rest[i];
        if (std.mem.eql(u8, a, "--timeout")) {
            i += 1;
            if (i >= rest.len) usageExit();
            timeout_s = std.fmt.parseFloat(f64, rest[i]) catch
                fail("bad --timeout value '{s}'", .{rest[i]});
        } else if (std.mem.eql(u8, a, "--any")) {
            any = true;
        } else if (std.mem.eql(u8, a, "--from")) {
            i += 1;
            if (i >= rest.len) usageExit();
            after = std.fmt.parseUnsigned(u64, rest[i], 10) catch
                fail("bad --from value '{s}'", .{rest[i]});
        } else {
            chans.append(arena, a) catch fail("out of memory", .{});
        }
    }
    if (chans.items.len == 0) usageExit();
    if (!any and chans.items.len > 1) usageExit();
    if (after != null and any) usageExit();
    if (after != null and chans.items.len != 1) usageExit();

    // A bare `recv` (no --timeout) must not block forever: apply a default
    // finite wait so agents can't hang. Overridable via FX_AGENT_BUS_RECV_TIMEOUT.
    if (timeout_s == null) {
        timeout_s = defaultRecvTimeout(env) catch 60.0;
    }

    const timeout_ms: ?i64 = if (timeout_s) |s| @intFromFloat(s * 1000.0) else null;
    const req = if (any)
        try protocol.encodeRecvAny(arena, chans.items, timeout_ms)
    else if (after) |a|
        try protocol.encodeRecvFrom(arena, chans.items[0], a, timeout_ms)
    else
        try protocol.encodeRecv(arena, chans.items[0], timeout_ms);

    const resp = roundtrip(io, arena, sock_path, req, timeout_ms);
    exitOnErr(resp);
    if (resp.empty orelse false) {
        // Timed out (or was told not to wait) — nonzero exit so shell callers
        // can branch on it.
        std.debug.print("fx-agent-bus: no message (timeout)\n", .{});
        std.process.exit(1);
    }
    printOut(io, arena, "{s}\n", .{resp.body.?});
}

/// Socket path: $FX_AGENT_BUS_SOCK, else $HOME/.config/hax/bus.sock.
fn sockPath(arena: Allocator, env: *std.process.Environ.Map) ![]const u8 {
    if (env.get("FX_AGENT_BUS_SOCK")) |s| return s;
    const home = env.get("HOME") orelse return error.NoHome;
    return std.fmt.allocPrint(arena, "{s}/.config/hax/bus.sock", .{home});
}

/// Hard cap (ms) for a parked recv wait: $FX_AGENT_BUS_MAX_WAIT seconds,
/// default 300s. "0" disables the cap (waits forever again).
fn maxWaitMs(arena: Allocator, env: *std.process.Environ.Map) ?i64 {
    _ = arena;
    const default_s: i64 = 300;
    if (env.get("FX_AGENT_BUS_MAX_WAIT")) |v| {
        const s = std.fmt.parseInt(i64, v, 10) catch return default_s * 1000;
        if (s <= 0) return null; // explicit opt out of the cap
        return s * 1000;
    }
    return default_s * 1000;
}

/// Default finite wait (seconds) applied when `recv` gets no --timeout.
/// $FX_AGENT_BUS_RECV_TIMEOUT overrides; default 60.
fn defaultRecvTimeout(env: *std.process.Environ.Map) !f64 {
    if (env.get("FX_AGENT_BUS_RECV_TIMEOUT")) |v| {
        const f = std.fmt.parseFloat(f64, v) catch return 60.0;
        if (f > 0) return f;
    }
    return 60.0;
}

fn readStdin(io: std.Io, arena: Allocator) []const u8 {
    var buffered: [4096]u8 = undefined;
    var r = std.Io.File.stdin().reader(io, &buffered);
    return r.interface.allocRemaining(arena, .limited(protocol.max_frame)) catch
        fail("failed to read body from stdin", .{});
}

/// One client roundtrip: connect, send frame, read frame. `recv_wait_ms` is
/// the wait the caller asked for; the client gives the daemon that plus a 10s
/// grace to answer, then bails rather than hang forever.
fn roundtrip(
    io: std.Io,
    arena: Allocator,
    sock_path: []const u8,
    req: []const u8,
    recv_wait_ms: ?i64,
) protocol.Response {
    daemon.ensureDaemon(io, arena, sock_path) catch |e|
        fail("cannot reach bus daemon at {s} ({s})", .{ sock_path, @errorName(e) });

    const ua = std.Io.net.UnixAddress.init(sock_path) catch |e|
        fail("bad socket path {s} ({s})", .{ sock_path, @errorName(e) });
    const stream = ua.connect(io) catch |e|
        fail("cannot connect to bus daemon at {s} ({s})", .{ sock_path, @errorName(e) });
    defer stream.socket.close(io);

    protocol.writeFrame(stream, io, req) catch |e|
        fail("failed to send request ({s})", .{@errorName(e)});

    if (recv_wait_ms) |wait_ms| {
        var pfd = [_]std.posix.pollfd{.{
            .fd = stream.socket.handle,
            .events = std.posix.POLL.IN,
            .revents = 0,
        }};
        const grace = wait_ms + 10_000;
        const r = std.posix.poll(&pfd, @intCast(@min(grace, std.math.maxInt(i32)))) catch 0;
        if (r == 0) fail("daemon did not answer within the recv deadline", .{});
    }

    const bytes = protocol.readFrame(stream.socket.handle, arena) catch |e| switch (e) {
        error.FrameTooLarge => fail("daemon sent an oversized frame", .{}),
        else => fail("lost connection to bus daemon ({s})", .{@errorName(e)}),
    };
    return protocol.decodeResponse(arena, bytes) catch
        fail("daemon sent an unparseable response", .{});
}

fn exitOnErr(resp: protocol.Response) void {
    if (resp.ok) return;
    if (resp.err) |msg| {
        std.debug.print("fx-agent-bus: {s}\n", .{msg});
    } else {
        std.debug.print("fx-agent-bus: daemon error\n", .{});
    }
    std.process.exit(1);
}

fn printOut(io: std.Io, arena: Allocator, comptime fmt: []const u8, args: anytype) void {
    const s = std.fmt.allocPrint(arena, fmt, args) catch return;
    std.Io.File.stdout().writeStreamingAll(io, s) catch {};
}
