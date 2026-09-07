// fx-agent-bus daemon: a single-threaded poll() loop over an AF_UNIX
// SOCK_STREAM socket, plus the client-side autostart helper.
//
// Concurrency model: one process, no threads. Every connection carries at most
// one request (CLI clients connect, send one frame, read one reply, exit).
// The only long-lived connections are parked `recv` waiters, which are kept in
// the poll set solely to observe EOF (client death) and deadlines.
//
// Sockets stay blocking. On POLLIN we read the whole frame synchronously —
// safe because a well-behaved client writes the full frame before reading the
// reply, and frames are capped at 1 MiB. Writes go through Stream.Writer
// (there is no std.posix.write in 0.16); EPIPE surfaces as error.WriteFailed
// because the default Io installs a no-op SIGPIPE handler.

const std = @import("std");
const protocol = @import("protocol.zig");
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// Bus: channels = FIFO of messages + list of parked waiter conn slots
// ---------------------------------------------------------------------------

pub const Bus = struct {
    alloc: Allocator,
    channels: std.StringHashMap(Channel),
    next_msg_id: u64 = 1,
    /// Per-channel transcript bound: the newest `history_cap` sent messages
    /// are retained (regardless of whether a waiter consumed them) so agents
    /// can catch up via `history <ch> --after <id>`.
    history_cap: usize = default_history_cap,
    pub const default_history_cap: usize = 100;

    pub const Channel = struct {
        fifo: std.ArrayList(Msg) = .empty,
        /// Conn slots (see Daemon.conns) blocked in recv on this channel,
        /// in the order they parked. The head waiter is served first.
        parked: std.ArrayList(usize) = .empty,
        /// Bounded transcript of the last `history_cap` messages sent on this
        /// channel, oldest first. Independent of the fifo: an entry survives
        /// even after a live consumer reads it, so a late joiner can re-read.
        history: std.ArrayList(Msg) = .empty,
    };

    pub const Msg = struct {
        id: u64,
        ts_ms: i64,
        /// Owned by the bus until popped; ownership transfers to the caller.
        body: []u8,
    };

    /// A parked waiter that was handed a message by send().
    pub const Wakeup = struct {
        slot: usize,
        /// Channel name (map key, valid while the channel exists).
        ch: []const u8,
        msg: Msg,
    };

    pub const SendResult = struct {
        id: u64,
        /// Fifo length after the send (single delivery to a lone waiter never
        /// enters the fifo; fan-out / no-waiter sends leave one copy queued).
        queued: usize,
        /// Caller frees the slice and each msg.body.
        wakeups: []Wakeup,
    };

    pub fn init(alloc: Allocator) Bus {
        return .{ .alloc = alloc, .channels = std.StringHashMap(Channel).init(alloc) };
    }

    pub fn deinit(bus: *Bus) void {
        var it = bus.channels.iterator();
        while (it.next()) |entry| {
            const ch = entry.value_ptr;
            for (ch.fifo.items) |m| bus.alloc.free(m.body);
            ch.fifo.deinit(bus.alloc);
            for (ch.history.items) |m| bus.alloc.free(m.body);
            ch.history.deinit(bus.alloc);
            ch.parked.deinit(bus.alloc);
            bus.alloc.free(entry.key_ptr.*);
        }
        bus.channels.deinit();
        bus.* = undefined;
    }

    fn getOrCreateChannel(bus: *Bus, name: []const u8) !*Channel {
        if (bus.channels.getPtr(name)) |c| return c;
        const key = try bus.alloc.dupe(u8, name);
        errdefer bus.alloc.free(key);
        const gop = try bus.channels.getOrPut(key);
        gop.value_ptr.* = .{};
        return gop.value_ptr;
    }

    /// Deliver body to a channel. Delivery fans out (one copy per parked
    /// waiter, all sharing one msg id, plus a copy retained in the fifo for
    /// later consumers) when `multi` is true OR when more than one waiter is
    /// parked — a single hand-off to the head would strand already-parked
    /// companions and leave nothing queued for a late joiner. With 0 or 1
    /// parked waiter, single delivery applies: with one waiter the head
    /// (FIFO) receives it directly (never touching the fifo); with none it is
    /// appended.
    pub fn send(bus: *Bus, name: []const u8, body: []const u8, now_ms: i64, multi: bool) !SendResult {
        const ch = try bus.getOrCreateChannel(name);
        const id = bus.next_msg_id;
        bus.next_msg_id += 1;

        // Fan out (one copy per parked waiter + a retained FIFO copy) when the
        // caller asks for it OR when more than one waiter is parked on the
        // channel: with multiple listeners a single hand-off to the head would
        // strand the already-parked companions, and a late joiner would see
        // nothing queued. With 0 or 1 waiter, single delivery applies.
        if (!multi and ch.parked.items.len <= 1) {
            if (ch.parked.items.len == 1) {
                const slot = ch.parked.orderedRemove(0);
                const msg_body = try bus.alloc.dupe(u8, body);
                errdefer bus.alloc.free(msg_body);
                var wakeups: std.ArrayList(Wakeup) = .empty;
                errdefer wakeups.deinit(bus.alloc);
                try wakeups.append(bus.alloc, .{
                    .slot = slot,
                    .ch = name,
                    .msg = .{ .id = id, .ts_ms = now_ms, .body = msg_body },
                });
                // Delivery committed; transcript recording is best-effort.
                bus.recordHistory(ch, id, now_ms, body) catch {};
                return .{ .id = id, .queued = ch.fifo.items.len, .wakeups = try wakeups.toOwnedSlice(bus.alloc) };
            }
            const msg_body = try bus.alloc.dupe(u8, body);
            errdefer bus.alloc.free(msg_body);
            try ch.fifo.append(bus.alloc, .{ .id = id, .ts_ms = now_ms, .body = msg_body });
            bus.recordHistory(ch, id, now_ms, body) catch {};
            return .{ .id = id, .queued = ch.fifo.items.len, .wakeups = &.{} };
        }

        // Multi fan-out: every allocation is freed on any error path by the
        // single defer; `retained` is nulled the moment the fifo takes over
        // its buffer, so it is never double-freed. `ok` only flips after the
        // wakeups slice ownership has moved to the caller.
        var wakeups: std.ArrayList(Wakeup) = .empty;
        var retained: ?[]u8 = null;
        var ok = false;
        defer {
            if (!ok) {
                for (wakeups.items) |wu| bus.alloc.free(wu.msg.body);
                if (retained) |r| bus.alloc.free(r);
                wakeups.deinit(bus.alloc);
            }
        }
        while (ch.parked.items.len > 0) {
            const slot = ch.parked.orderedRemove(0);
            const msg_body = try bus.alloc.dupe(u8, body);
            errdefer bus.alloc.free(msg_body);
            try wakeups.append(bus.alloc, .{
                .slot = slot,
                .ch = name,
                .msg = .{ .id = id, .ts_ms = now_ms, .body = msg_body },
            });
        }
        const copy = try bus.alloc.dupe(u8, body);
        retained = copy;
        try ch.fifo.append(bus.alloc, .{ .id = id, .ts_ms = now_ms, .body = copy });
        retained = null; // ownership moved into the fifo
        const wu = try wakeups.toOwnedSlice(bus.alloc);
        ok = true;
        bus.recordHistory(ch, id, now_ms, body) catch {};
        return .{ .id = id, .queued = ch.fifo.items.len, .wakeups = wu };
    }

    /// Append `body` to the channel transcript, evicting the oldest entry once
    /// the history is over `history_cap`. History is independent of the fifo,
    /// so an entry persists after a live consumer reads it.
    fn recordHistory(bus: *Bus, ch: *Channel, id: u64, ts_ms: i64, body: []const u8) !void {
        const b = try bus.alloc.dupe(u8, body);
        errdefer bus.alloc.free(b);
        try ch.history.append(bus.alloc, .{ .id = id, .ts_ms = ts_ms, .body = b });
        while (ch.history.items.len > bus.history_cap) {
            const old = ch.history.orderedRemove(0);
            bus.alloc.free(old.body);
        }
    }

    /// Snapshot the transcript of `name`: messages with `id > after` if
    /// `after` is given, else the whole retained history. Caller frees the
    /// slice and each msg.body. Empty when the channel has no matching history.
    pub fn historySince(bus: *Bus, name: []const u8, after: ?u64, alloc: Allocator) ![]protocol.HistoryMsg {
        var out: std.ArrayList(protocol.HistoryMsg) = .empty;
        errdefer out.deinit(alloc);
        const ch = bus.channels.getPtr(name) orelse return try out.toOwnedSlice(alloc);
        for (ch.history.items) |m| {
            if (after) |a| {
                if (m.id <= a) continue;
            }
            const copy = try alloc.dupe(u8, m.body);
            errdefer alloc.free(copy);
            try out.append(alloc, .{ .id = m.id, .ts = m.ts_ms, .body = copy });
        }
        return out.toOwnedSlice(alloc);
    }

    /// Pop the oldest message from a channel, if any. Caller owns msg.body.
    pub fn popFront(bus: *Bus, name: []const u8) ?Msg {
        const ch = bus.channels.getPtr(name) orelse return null;
        if (ch.fifo.items.len == 0) return null;
        return ch.fifo.orderedRemove(0);
    }

    /// Non-destructive: return the earliest transcript message with id >
    /// `after` (a copy; the entry stays in history for other cursor readers).
    /// Caller owns msg.body. Null when the channel has no such retained message.
    pub fn nextAfter(bus: *Bus, name: []const u8, after: u64, alloc: Allocator) !?Msg {
        const ch = bus.channels.getPtr(name) orelse return null;
        for (ch.history.items) |m| {
            if (m.id <= after) continue;
            const copy = try alloc.dupe(u8, m.body);
            return Msg{ .id = m.id, .ts_ms = m.ts_ms, .body = copy };
        }
        return null;
    }

    pub fn queueLen(bus: *Bus, name: []const u8) usize {
        const ch = bus.channels.getPtr(name) orelse return 0;
        return ch.fifo.items.len;
    }

    /// Register a conn slot as parked on a channel (created if missing).
    pub fn park(bus: *Bus, name: []const u8, slot: usize) !void {
        const ch = try bus.getOrCreateChannel(name);
        try ch.parked.append(bus.alloc, slot);
    }

    /// Remove a conn slot from every parked list (disconnect, wake, or re-park).
    pub fn unparkSlot(bus: *Bus, slot: usize) void {
        var it = bus.channels.iterator();
        while (it.next()) |entry| {
            const parked = &entry.value_ptr.parked;
            var i: usize = 0;
            while (i < parked.items.len) {
                if (parked.items[i] == slot) {
                    _ = parked.orderedRemove(i);
                } else {
                    i += 1;
                }
            }
        }
    }

    /// Drop a channel and every message queued on it. Returns false if the
    /// channel does not exist. Parked waiters are left alone; the daemon
    /// answers them itself.
    pub fn closeChannel(bus: *Bus, name: []const u8) bool {
        const ch = bus.channels.getPtr(name) orelse return false;
        for (ch.fifo.items) |m| bus.alloc.free(m.body);
        ch.fifo.deinit(bus.alloc);
        for (ch.history.items) |m| bus.alloc.free(m.body);
        ch.history.deinit(bus.alloc);
        ch.parked.deinit(bus.alloc);
        // Remove the map entry; the key is owned by (and freed with) the map.
        _ = bus.channels.remove(name);
        return true;
    }

    /// Snapshot of all channels sorted by name. Caller frees the slice.
    pub fn listChannels(bus: *Bus, alloc: Allocator) ![]protocol.ChannelInfo {
        var out: std.ArrayList(protocol.ChannelInfo) = .empty;
        errdefer out.deinit(alloc);
        var it = bus.channels.iterator();
        while (it.next()) |entry| {
            try out.append(alloc, .{
                .ch = entry.key_ptr.*,
                .queued = entry.value_ptr.fifo.items.len,
                .parked = entry.value_ptr.parked.items.len,
            });
        }
        const items = try out.toOwnedSlice(alloc);
        std.mem.sort(protocol.ChannelInfo, items, {}, struct {
            fn less(_: void, a: protocol.ChannelInfo, b: protocol.ChannelInfo) bool {
                return std.mem.lessThan(u8, a.ch, b.ch);
            }
        }.less);
        return items;
    }
};

// ---------------------------------------------------------------------------
// Daemon
// ---------------------------------------------------------------------------

const Conn = struct {
    stream: std.Io.net.Stream,
    /// Per-connection arena: holds the request frame and response buffers.
    /// One request per conn, so it is simply torn down on close.
    arena: std.heap.ArenaAllocator,
    /// True while this conn is a parked recv waiter.
    parked: bool = false,
    /// Absolute wall-clock deadline (ms) when parked with a timeout.
    deadline_ms: ?i64 = null,
};

const Daemon = struct {
    io: std.Io,
    alloc: Allocator,
    sock_path: []const u8,
    bus: Bus,
    /// Live conns by slot index; null = free slot. Slots are stable so parked
    /// lists can reference them across iterations.
    conns: std.ArrayList(?Conn) = .empty,
    start_ms: i64,
    /// Hard ceiling (ms) on how long a parked recv may wait; null disables the
    /// cap (not the default). Set from FX_AGENT_BUS_MAX_WAIT.
    max_wait_ms: ?i64,
    shutdown_requested: bool = false,

    fn now(d: *const Daemon) i64 {
        return nowMs(d.io);
    }
};

pub fn nowMs(io: std.Io) i64 {
    const ts = std.Io.Clock.Timestamp.now(io, .real);
    return @intCast(@divTrunc(ts.raw.nanoseconds, std.time.ns_per_ms));
}

/// Run the daemon until a `shutdown` request arrives. Returns only on
/// graceful shutdown (or a fatal setup error).
pub fn run(io: std.Io, alloc: Allocator, sock_path: []const u8, max_wait_ms: ?i64) !void {
    // Clear a stale socket file left by a crashed predecessor; ignore
    // "not there". (Two daemons racing on autostart can unlink each other's
    // live socket — accepted for v1: single-user bus, sequential agents.)
    std.Io.Dir.deleteFileAbsolute(io, sock_path) catch |e| switch (e) {
        error.FileNotFound => {},
        else => return e,
    };
    const ua = try std.Io.net.UnixAddress.init(sock_path);
    var server = try ua.listen(io, .{});
    defer server.deinit(io);

    var d = Daemon{
        .io = io,
        .alloc = alloc,
        .sock_path = sock_path,
        .bus = Bus.init(alloc),
        .start_ms = nowMs(io),
        .max_wait_ms = max_wait_ms,
    };
    defer {
        for (d.conns.items, 0..) |maybe, slot| {
            if (maybe != null) closeConn(&d, slot);
        }
        d.conns.deinit(alloc);
        d.bus.deinit();
    }

    while (!d.shutdown_requested) {
        // Expire parked deadlines; find the earliest remaining one.
        const now = d.now();
        var earliest: ?i64 = null;
        for (d.conns.items, 0..) |maybe, slot| {
            const c = maybe orelse continue;
            if (!c.parked) continue;
            const dl = c.deadline_ms orelse continue;
            if (dl <= now) {
                const resp = protocol.encodeEmptyOk(connArena(&d, slot)) catch {
                    closeConn(&d, slot);
                    continue;
                };
                respond(&d, slot, resp) catch {};
                closeConn(&d, slot);
            } else if (earliest == null or dl < earliest.?) {
                earliest = dl;
            }
        }

        // Build the poll set: [0] = listener, then one entry per live conn.
        var fds: std.ArrayList(std.posix.pollfd) = .empty;
        defer fds.deinit(alloc);
        var slots: std.ArrayList(usize) = .empty;
        defer slots.deinit(alloc);
        try fds.append(alloc, .{ .fd = server.socket.handle, .events = std.posix.POLL.IN, .revents = 0 });
        try slots.append(alloc, 0); // placeholder keeps slots[] aligned with fds[]
        for (d.conns.items, 0..) |maybe, slot| {
            const c = maybe orelse continue;
            try fds.append(alloc, .{ .fd = c.stream.socket.handle, .events = std.posix.POLL.IN, .revents = 0 });
            try slots.append(alloc, slot);
        }

        const timeout: i32 = if (earliest) |dl|
            @intCast(@min(@max(dl - now, 0), 1000))
        else
            1000;
        _ = std.posix.poll(fds.items, timeout) catch continue;

        if (fds.items[0].revents != 0) {
            if (server.accept(io)) |stream| {
                // The client's request (if already sent) is picked up next
                // poll iteration.
                _ = takeSlot(&d, stream) catch {
                    stream.socket.close(io);
                };
            } else |_| {}
        }

        for (fds.items[1..], slots.items[1..]) |pfd, slot| {
            if (d.conns.items[slot] == null) continue;
            if (pfd.revents & (std.posix.POLL.HUP | std.posix.POLL.ERR | std.posix.POLL.NVAL) != 0) {
                closeConn(&d, slot);
                continue;
            }
            if (pfd.revents & std.posix.POLL.IN == 0) continue;
            const c = &d.conns.items[slot].?;
            if (c.parked) {
                // Readable while parked = client closed its end (EOF) — a
                // parked conn never has unread request bytes. Anything else
                // is a protocol violation; either way drop the waiter.
                var probe: [1]u8 = undefined;
                const n = std.posix.read(c.stream.socket.handle, &probe) catch 0;
                if (n > 0) std.debug.print("fx-agent-bus: protocol violation on parked conn, dropping\n", .{});
                closeConn(&d, slot);
            } else {
                serveConn(&d, slot);
            }
        }
    }

    // Graceful shutdown: conns are closed by the deferred cleanup above.
    std.Io.Dir.deleteFileAbsolute(io, sock_path) catch {};
}

fn connArena(d: *Daemon, slot: usize) Allocator {
    return d.conns.items[slot].?.arena.allocator();
}

fn takeSlot(d: *Daemon, stream: std.Io.net.Stream) !usize {
    for (d.conns.items, 0..) |maybe, slot| {
        if (maybe == null) {
            d.conns.items[slot] = .{ .stream = stream, .arena = std.heap.ArenaAllocator.init(d.alloc) };
            return slot;
        }
    }
    try d.conns.append(d.alloc, .{ .stream = stream, .arena = std.heap.ArenaAllocator.init(d.alloc) });
    return d.conns.items.len - 1;
}

fn closeConn(d: *Daemon, slot: usize) void {
    const maybe = d.conns.items[slot] orelse return;
    d.bus.unparkSlot(slot);
    maybe.stream.socket.close(d.io);
    var arena = maybe.arena;
    arena.deinit();
    d.conns.items[slot] = null;
}

fn respond(d: *Daemon, slot: usize, resp: []const u8) !void {
    if (d.conns.items[slot] == null) return error.ConnGone;
    const c = &d.conns.items[slot].?;
    protocol.writeFrame(c.stream, d.io, resp) catch {
        closeConn(d, slot);
        return error.WriteFailed;
    };
}

fn respondErr(d: *Daemon, slot: usize, msg: []const u8) void {
    const resp = protocol.encodeErr(connArena(d, slot), msg) catch return;
    respond(d, slot, resp) catch {};
}

fn serveConn(d: *Daemon, slot: usize) void {
    if (d.conns.items[slot] == null) return;
    const conn = &d.conns.items[slot].?;
    const frame = protocol.readFrame(conn.stream.socket.handle, conn.arena.allocator()) catch |e| switch (e) {
        error.FrameTooLarge => {
            respondErr(d, slot, "frame too large");
            closeConn(d, slot);
            return;
        },
        else => {
            // EOF / read error / OOM — client went away or died mid-frame.
            closeConn(d, slot);
            return;
        },
    };
    dispatch(d, slot, frame);
    // Non-parked conns were closed by dispatch after responding; parked
    // conns stay in the poll set.
}

fn dispatch(d: *Daemon, slot: usize, frame: []const u8) void {
    const req = protocol.decodeRequest(connArena(d, slot), frame) catch {
        respondErr(d, slot, "malformed request json");
        closeConn(d, slot);
        return;
    };

    if (std.mem.eql(u8, req.op, "send")) {
        opSend(d, slot, &req);
    } else if (std.mem.eql(u8, req.op, "recv")) {
        opRecv(d, slot, &req);
    } else if (std.mem.eql(u8, req.op, "poll")) {
        opPoll(d, slot, &req);
    } else if (std.mem.eql(u8, req.op, "list")) {
        opList(d, slot);
    } else if (std.mem.eql(u8, req.op, "close")) {
        opClose(d, slot, &req);
    } else if (std.mem.eql(u8, req.op, "history")) {
        opHistory(d, slot, &req);
    } else if (std.mem.eql(u8, req.op, "who")) {
        opWho(d, slot);
    } else if (std.mem.eql(u8, req.op, "shutdown")) {
        const resp = protocol.encodeOk(connArena(d, slot)) catch return;
        respond(d, slot, resp) catch {};
        d.shutdown_requested = true;
        closeConn(d, slot);
    } else if (std.mem.eql(u8, req.op, "ping")) {
        const resp = protocol.encodeOk(connArena(d, slot)) catch return;
        respond(d, slot, resp) catch {};
        closeConn(d, slot);
    } else {
        respondErr(d, slot, "unknown op");
        closeConn(d, slot);
    }
}

fn opSend(d: *Daemon, slot: usize, req: *const protocol.Request) void {
    const ch = req.ch orelse {
        respondErr(d, slot, "send requires ch");
        closeConn(d, slot);
        return;
    };
    const body = req.body orelse {
        respondErr(d, slot, "send requires body");
        closeConn(d, slot);
        return;
    };
    const res = d.bus.send(ch, body, d.now(), req.multi orelse false) catch {
        respondErr(d, slot, "out of memory");
        closeConn(d, slot);
        return;
    };
    defer d.alloc.free(res.wakeups);

    // Answer the sender first.
    if (protocol.encodeSendOk(connArena(d, slot), res.id, res.queued, res.wakeups.len)) |resp| {
        respond(d, slot, resp) catch {}; // respond() closes the conn on failure
    } else |_| {}
    closeConn(d, slot);

    // Hand each woken waiter its message and close its conn (one request per
    // conn). closeConn unparks the slot from any other channel lists too.
    for (res.wakeups) |wu| {
        defer d.alloc.free(wu.msg.body);
        if (d.conns.items[wu.slot] == null) continue;
        const wresp = protocol.encodeRecvOk(connArena(d, wu.slot), wu.ch, wu.msg.body, wu.msg.ts_ms, wu.msg.id) catch continue;
        respond(d, wu.slot, wresp) catch continue; // closes on failure
        closeConn(d, wu.slot);
    }
}

fn opRecv(d: *Daemon, slot: usize, req: *const protocol.Request) void {
    // Channels this recv applies to; `single` borrows from the request (which
    // lives in the conn arena), never from a local.
    var single: [1][]const u8 = undefined;
    var names: []const []const u8 = undefined;
    if (req.ch) |ch| {
        single[0] = ch;
        names = &single;
    } else if (req.ch_any) |list| {
        if (list.len == 0) {
            respondErr(d, slot, "recv requires a non-empty ch_any");
            closeConn(d, slot);
            return;
        }
        names = list;
    } else {
        respondErr(d, slot, "recv requires ch or ch_any");
        closeConn(d, slot);
        return;
    }

    // `--from <id>`: catch-up then live. Cursor reads only a single channel
    // (replay order is unambiguous); it replays the oldest transcript message
    // newer than the cursor, else parks for the next send (whose global id is
    // always greater than any cursor).
    if (req.after) |after| {
        if (names.len != 1) {
            respondErr(d, slot, "--from requires a single channel");
            closeConn(d, slot);
            return;
        }
        const maybe = d.bus.nextAfter(names[0], after, d.alloc) catch {
            respondErr(d, slot, "out of memory");
            closeConn(d, slot);
            return;
        };
        if (maybe) |msg| {
            defer d.alloc.free(msg.body);
            const resp = protocol.encodeRecvOk(connArena(d, slot), names[0], msg.body, msg.ts_ms, msg.id) catch {
                closeConn(d, slot);
                return;
            };
            respond(d, slot, resp) catch {};
            closeConn(d, slot);
            return;
        }
        // Nothing retained beyond the cursor. Every sent message is also on the
        // transcript, so no fifo message can have id > after either — a cursor
        // read must never pop the fifo (queued entries may be at or behind the
        // cursor). Park for the next send, whose global id is always > after.
    } else {
        // Immediate attempt over every requested channel, in order. Only for a
        // plain recv (no cursor): fifo messages may be at or behind a cursor.
        for (names) |name| {
            if (d.bus.popFront(name)) |msg| {
                defer d.alloc.free(msg.body);
                const resp = protocol.encodeRecvOk(connArena(d, slot), name, msg.body, msg.ts_ms, msg.id) catch {
                    closeConn(d, slot);
                    return;
                };
                respond(d, slot, resp) catch {};
                closeConn(d, slot);
                return;
            }
        }
    }

    // Nothing queued: timeout_ms <= 0 answers empty right now.
    if (req.timeout_ms) |t| {
        if (t <= 0) {
            const resp = protocol.encodeEmptyOk(connArena(d, slot)) catch {
                closeConn(d, slot);
                return;
            };
            respond(d, slot, resp) catch {};
            closeConn(d, slot);
            return;
        }
    }

    // Park on every requested channel until a send or the deadline.
    for (names) |name| {
        d.bus.park(name, slot) catch {
            d.bus.unparkSlot(slot);
            respondErr(d, slot, "out of memory");
            closeConn(d, slot);
            return;
        };
    }
    const c = &d.conns.items[slot].?;
    c.parked = true;
    // Bound the wait so no parked recv blocks forever, even if the caller
    // omitted --timeout or asked for a huge one. deadline_ms is null only when
    // max_wait_ms is null (explicitly disabled).
    c.deadline_ms = if (d.max_wait_ms) |cap|
        d.now() + waitFor(req.timeout_ms, cap)
    else if (req.timeout_ms) |t|
        d.now() + t
    else
        null;
    // Stay connected: the poll set keeps watching for EOF and deadlines.
}

/// Clamp a requested wait to the daemon cap. A missing/<=0 request with a cap
/// waits the cap; with a cap, a caller cannot ask to wait forever.
fn waitFor(req_ms: ?i64, cap_ms: i64) i64 {
    if (req_ms) |t| {
        if (t <= 0) return cap_ms;
        return @min(t, cap_ms);
    }
    return cap_ms;
}

fn opPoll(d: *Daemon, slot: usize, req: *const protocol.Request) void {
    const ch = req.ch orelse {
        respondErr(d, slot, "poll requires ch");
        closeConn(d, slot);
        return;
    };
    const resp = protocol.encodePollOk(connArena(d, slot), d.bus.queueLen(ch)) catch {
        closeConn(d, slot);
        return;
    };
    respond(d, slot, resp) catch {};
    closeConn(d, slot);
}

fn opHistory(d: *Daemon, slot: usize, req: *const protocol.Request) void {
    const ch = req.ch orelse {
        respondErr(d, slot, "history requires ch");
        closeConn(d, slot);
        return;
    };
    const msgs = d.bus.historySince(ch, req.after, d.alloc) catch {
        respondErr(d, slot, "out of memory");
        closeConn(d, slot);
        return;
    };
    defer d.alloc.free(msgs);
    defer for (msgs) |m| d.alloc.free(m.body);
    const resp = protocol.encodeHistoryOk(connArena(d, slot), ch, msgs) catch {
        closeConn(d, slot);
        return;
    };
    respond(d, slot, resp) catch {};
    closeConn(d, slot);
}

fn opList(d: *Daemon, slot: usize) void {
    const infos = d.bus.listChannels(d.alloc) catch {
        respondErr(d, slot, "out of memory");
        closeConn(d, slot);
        return;
    };
    defer d.alloc.free(infos);
    const resp = protocol.encodeListOk(connArena(d, slot), infos) catch {
        closeConn(d, slot);
        return;
    };
    respond(d, slot, resp) catch {};
    closeConn(d, slot);
}

fn opClose(d: *Daemon, slot: usize, req: *const protocol.Request) void {
    const ch = req.ch orelse {
        respondErr(d, slot, "close requires ch");
        closeConn(d, slot);
        return;
    };
    if (!d.bus.closeChannel(ch)) {
        respondErr(d, slot, "no such channel");
        closeConn(d, slot);
        return;
    }
    const resp = protocol.encodeOk(connArena(d, slot)) catch {
        closeConn(d, slot);
        return;
    };
    respond(d, slot, resp) catch {};
    closeConn(d, slot);
}

fn opWho(d: *Daemon, slot: usize) void {
    const pid: i64 = @intCast(std.os.linux.getpid());
    const uptime: u64 = @intCast(@max(d.now() - d.start_ms, 0));
    const resp = protocol.encodeWhoOk(connArena(d, slot), pid, uptime, d.sock_path) catch {
        closeConn(d, slot);
        return;
    };
    respond(d, slot, resp) catch {};
    closeConn(d, slot);
}

// ---------------------------------------------------------------------------
// Client-side autostart
// ---------------------------------------------------------------------------

fn probeConnect(io: std.Io, sock_path: []const u8) !std.Io.net.Stream {
    const ua = try std.Io.net.UnixAddress.init(sock_path);
    return ua.connect(io);
}

fn socketFileExists(io: std.Io, sock_path: []const u8) bool {
    _ = std.Io.Dir.cwd().statFile(io, sock_path, .{}) catch return false;
    return true;
}

/// Make sure a daemon is listening: try to connect; on failure unlink any
/// stale socket file, spawn a detached daemon (own process group, stdio to
/// /dev/null), and retry connecting ~50x20ms (~1s total).
pub fn ensureDaemon(io: std.Io, alloc: Allocator, sock_path: []const u8) !void {
    var spawned = false;
    var attempt: usize = 0;
    while (attempt < 50) : (attempt += 1) {
        // Only probe when the socket file exists: a missing path would make
        // std's connect surface refused/unexpected errno noise for nothing.
        if (socketFileExists(io, sock_path)) {
            if (probeConnect(io, sock_path)) |stream| {
                stream.socket.close(io);
                return;
            } else |err| switch (err) {
                // Path exists but nothing listens behind it (stale socket
                // from a crashed daemon). 0.16's Threaded Io maps
                // ECONNREFUSED to error.Unexpected.
                error.Unexpected => std.Io.Dir.deleteFileAbsolute(io, sock_path) catch {},
                else => {},
            }
        }
        if (!spawned) {
            spawned = true;
            const self = std.process.executablePathAlloc(io, alloc) catch null;
            if (self) |self_path| {
                defer alloc.free(self_path);
                _ = std.process.spawn(io, .{
                    .argv = &.{ self_path, "daemon" },
                    .pgid = 0,
                    .stdin = .ignore,
                    .stdout = .ignore,
                    .stderr = .ignore,
                }) catch {};
            }
        }
        io.sleep(std.Io.Duration.fromMilliseconds(20), .real) catch {};
    }
    return error.DaemonUnavailable;
}

// ---------------------------------------------------------------------------
// Unit tests (Bus only — no sockets; the IO paths are covered by
// tests/integration.sh)
// ---------------------------------------------------------------------------

const testing = std.testing;

test "bus fifo order across send and recv" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    _ = try bus.send("a", "one", 0, false);
    _ = try bus.send("a", "two", 0, false);
    _ = try bus.send("a", "three", 0, false);
    try testing.expectEqual(@as(usize, 3), bus.queueLen("a"));

    const m1 = bus.popFront("a").?;
    defer testing.allocator.free(m1.body);
    try testing.expectEqualStrings("one", m1.body);
    try testing.expectEqual(@as(u64, 1), m1.id);

    const m2 = bus.popFront("a").?;
    defer testing.allocator.free(m2.body);
    try testing.expectEqualStrings("two", m2.body);

    const m3 = bus.popFront("a").?;
    defer testing.allocator.free(m3.body);
    try testing.expectEqualStrings("three", m3.body);
    try testing.expect(bus.popFront("a") == null);
    try testing.expectEqual(@as(usize, 0), bus.queueLen("nope"));
}

test "bus single send with one parked waiter hands off directly, nothing queued" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    try bus.park("w", 7);

    const res = try bus.send("w", "msg", 42, false);
    defer testing.allocator.free(res.wakeups);
    try testing.expectEqual(@as(usize, 1), res.wakeups.len);
    try testing.expectEqual(@as(usize, 7), res.wakeups[0].slot);
    try testing.expectEqualStrings("w", res.wakeups[0].ch);
    try testing.expectEqualStrings("msg", res.wakeups[0].msg.body);
    try testing.expectEqual(@as(i64, 42), res.wakeups[0].msg.ts_ms);
    try testing.expectEqual(@as(usize, 0), res.queued);
    testing.allocator.free(res.wakeups[0].msg.body);

    // Single delivery: nothing retained for late joiners, no waiter stays parked.
    try testing.expectEqual(@as(usize, 0), bus.queueLen("w"));
    try testing.expectEqual(@as(usize, 0), bus.channels.getPtr("w").?.parked.items.len);
}

test "bus single send with no parked waiter just queues" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    const res = try bus.send("s", "queued-only", 0, false);
    defer testing.allocator.free(res.wakeups);
    try testing.expectEqual(@as(usize, 0), res.wakeups.len);
    try testing.expectEqual(@as(usize, 1), bus.queueLen("s"));
    const m = bus.popFront("s").?;
    defer testing.allocator.free(m.body);
    try testing.expectEqualStrings("queued-only", m.body);
    try testing.expectEqual(res.id, m.id);
}

test "bus multi send fans out to all parked waiters and retains a copy" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    try bus.park("m", 10);
    try bus.park("m", 11);
    try bus.park("m", 12);

    const res = try bus.send("m", "bcast", 77, true);
    defer testing.allocator.free(res.wakeups);
    try testing.expectEqual(@as(usize, 3), res.wakeups.len);
    for (res.wakeups, 0..) |wu, i| {
        try testing.expectEqual(@as(usize, 10 + i), wu.slot);
        try testing.expectEqualStrings("m", wu.ch);
        try testing.expectEqualStrings("bcast", wu.msg.body);
        try testing.expectEqual(res.id, wu.msg.id);
        try testing.expectEqual(@as(i64, 77), wu.msg.ts_ms);
    }
    // Each waiter received its own copy, not a shared slice.
    try testing.expect(res.wakeups[0].msg.body.ptr != res.wakeups[1].msg.body.ptr);
    try testing.expect(res.wakeups[1].msg.body.ptr != res.wakeups[2].msg.body.ptr);
    for (res.wakeups) |wu| testing.allocator.free(wu.msg.body);

    // A copy with the same id is retained for busy consumers.
    try testing.expectEqual(@as(usize, 1), bus.queueLen("m"));
    const retained = bus.popFront("m").?;
    defer testing.allocator.free(retained.body);
    try testing.expectEqualStrings("bcast", retained.body);
    try testing.expectEqual(res.id, retained.id);
    try testing.expectEqual(@as(i64, 77), retained.ts_ms);

    // No waiter stays parked.
    try testing.expectEqual(@as(usize, 0), bus.channels.getPtr("m").?.parked.items.len);
}

test "bus multi send with no parked waiters just queues" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    const res = try bus.send("solo", "queued-only", 0, true);
    defer testing.allocator.free(res.wakeups);
    try testing.expectEqual(@as(usize, 0), res.wakeups.len);
    try testing.expectEqual(@as(usize, 1), bus.queueLen("solo"));
    const m = bus.popFront("solo").?;
    defer testing.allocator.free(m.body);
    try testing.expectEqualStrings("queued-only", m.body);
    try testing.expectEqual(res.id, m.id);
}

test "bus single send with multiple parked waiters fans out and retains a copy" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    try bus.park("s", 1);
    try bus.park("s", 2);
    const res = try bus.send("s", "one-only", 0, false);
    defer testing.allocator.free(res.wakeups);
    // Two parked waiters: even without --multi, a single send fans out to both
    // so no parked companion is stranded, and retains a copy for late joiners.
    try testing.expectEqual(@as(usize, 2), res.wakeups.len);
    try testing.expectEqual(@as(usize, 1), res.wakeups[0].slot);
    try testing.expectEqual(@as(usize, 2), res.wakeups[1].slot);
    for (res.wakeups) |wu| testing.allocator.free(wu.msg.body);
    try testing.expectEqual(@as(usize, 1), bus.queueLen("s"));
    try testing.expectEqual(@as(usize, 0), bus.channels.getPtr("s").?.parked.items.len);
}

test "bus historySince returns messages after id, non-destructive" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    const r1 = try bus.send("h", "one", 1, false);
    const r2 = try bus.send("h", "two", 2, false);
    const r3 = try bus.send("h", "three", 3, false);

    // Full transcript, oldest first.
    const all = try bus.historySince("h", null, testing.allocator);
    defer {
        for (all) |m| testing.allocator.free(m.body);
        testing.allocator.free(all);
    }
    try testing.expectEqual(@as(usize, 3), all.len);
    try testing.expectEqual(r1.id, all[0].id);
    try testing.expectEqualStrings("one", all[0].body);
    try testing.expectEqualStrings("three", all[2].body);

    // After the second message -> only the third.
    const tail = try bus.historySince("h", r2.id, testing.allocator);
    defer {
        for (tail) |m| testing.allocator.free(m.body);
        testing.allocator.free(tail);
    }
    try testing.expectEqual(@as(usize, 1), tail.len);
    try testing.expectEqual(r3.id, tail[0].id);
    try testing.expectEqualStrings("three", tail[0].body);

    // History is independent of the fifo: after all are consumed it still exists.
    _ = bus.popFront("h").?;
    _ = bus.popFront("h").?;
    _ = bus.popFront("h").?;
    const still = try bus.historySince("h", null, testing.allocator);
    defer {
        for (still) |m| testing.allocator.free(m.body);
        testing.allocator.free(still);
    }
    try testing.expectEqual(@as(usize, 3), still.len);
}

test "bus history evicts oldest beyond history_cap" {
    var bus = Bus.init(testing.allocator);
    bus.history_cap = 3;
    defer bus.deinit();

    _ = try bus.send("h", "m1", 0, false);
    _ = try bus.send("h", "m2", 0, false);
    _ = try bus.send("h", "m3", 0, false);
    _ = try bus.send("h", "m4", 0, false);

    const hist = try bus.historySince("h", null, testing.allocator);
    defer {
        for (hist) |m| testing.allocator.free(m.body);
        testing.allocator.free(hist);
    }
    try testing.expectEqual(@as(usize, 3), hist.len);
    try testing.expectEqualStrings("m2", hist[0].body);
    try testing.expectEqualStrings("m4", hist[2].body);
}

test "bus nextAfter returns oldest transcript message newer than cursor" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    const r1 = try bus.send("h", "one", 1, false);
    const r2 = try bus.send("h", "two", 2, false);
    const r3 = try bus.send("h", "three", 3, false);

    const m = (try bus.nextAfter("h", r1.id, testing.allocator)).?;
    defer testing.allocator.free(m.body);
    try testing.expectEqual(r2.id, m.id);
    try testing.expectEqualStrings("two", m.body);

    const last = (try bus.nextAfter("h", r3.id, testing.allocator));
    try testing.expect(last == null);

    const fresh = (try bus.nextAfter("nope", 0, testing.allocator));
    try testing.expect(fresh == null);
}

test "bus history on unknown channel is empty" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();
    const hist = try bus.historySince("nope", null, testing.allocator);
    defer testing.allocator.free(hist);
    try testing.expectEqual(@as(usize, 0), hist.len);
}

test "bus closeChannel drops queued messages" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    _ = try bus.send("c", "x", 0, false);
    _ = try bus.send("c", "y", 0, false);
    try testing.expect(bus.closeChannel("c"));
    try testing.expectEqual(@as(usize, 0), bus.queueLen("c"));
    try testing.expect(!bus.closeChannel("c"));
}

test "bus listChannels sorted with counts" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    _ = try bus.send("b", "1", 0, false);
    _ = try bus.send("b", "2", 0, false);
    _ = try bus.send("a", "3", 0, false);
    try bus.park("b", 3);

    const infos = try bus.listChannels(testing.allocator);
    defer testing.allocator.free(infos);
    try testing.expectEqual(@as(usize, 2), infos.len);
    try testing.expectEqualStrings("a", infos[0].ch);
    try testing.expectEqual(@as(usize, 1), infos[0].queued);
    try testing.expectEqual(@as(usize, 0), infos[0].parked);
    try testing.expectEqualStrings("b", infos[1].ch);
    try testing.expectEqual(@as(usize, 2), infos[1].queued);
    try testing.expectEqual(@as(usize, 1), infos[1].parked);
}

test "bus parked recv on ch_any wakes via any member channel" {
    var bus = Bus.init(testing.allocator);
    defer bus.deinit();

    try bus.park("x", 5);
    try bus.park("y", 5);

    const res = try bus.send("y", "via-y", 0, false);
    defer testing.allocator.free(res.wakeups);
    try testing.expectEqual(@as(usize, 1), res.wakeups.len);
    try testing.expectEqualStrings("y", res.wakeups[0].ch);
    testing.allocator.free(res.wakeups[0].msg.body);
}
