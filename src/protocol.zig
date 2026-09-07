// fx-agent-bus wire protocol.
//
// Frame = u32 LE payload length N, then N bytes UTF-8 JSON. N is capped at
// max_frame (1 MiB) to bound memory use and blocking-write stall time.
//
// Request:  {"op":"send|recv|poll|list|close|who|shutdown|ping",
//            "ch":string?, "body":string?, "ch_any":[string]?, "timeout_ms":int?,
//            "multi":bool?}
// Response: {"ok":true, ...fields} | {"ok":false,"err":string}
//
// JSON encoding is hand-rolled (flat objects, string/int fields only): the
// 0.16 std.json stringify surface is value-oriented and adds nothing here,
// while parsing goes through std.json.parseFromSliceLeaky with a struct.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Unix socket paths are capped at 108 bytes by the kernel (sockaddr_un).
const unix_path_max = 108;

/// Largest allowed frame payload.
pub const max_frame: u32 = 1 << 20;

pub const FrameError = error{
    FrameTooLarge,
    EndOfStream,
    ReadFailed,
    WriteFailed,
    OutOfMemory,
};

// ---------------------------------------------------------------------------
// Frame IO
// ---------------------------------------------------------------------------

/// Read one frame from a (blocking) fd. The header and body tolerate partial
/// reads: both loops continue until the full byte count is consumed. EOF
/// before a complete frame yields error.EndOfStream. Caller owns the payload.
pub fn readFrame(fd: std.posix.fd_t, alloc: Allocator) FrameError![]u8 {
    var hdr: [4]u8 = undefined;
    try readAllFd(fd, &hdr);
    const n = std.mem.readInt(u32, &hdr, .little);
    if (n > max_frame) return error.FrameTooLarge;
    const buf = try alloc.alloc(u8, n);
    errdefer alloc.free(buf);
    try readAllFd(fd, buf);
    return buf;
}

fn readAllFd(fd: std.posix.fd_t, buf: []u8) FrameError!void {
    var i: usize = 0;
    while (i < buf.len) {
        const n = std.posix.read(fd, buf[i..]) catch return error.ReadFailed;
        if (n == 0) return error.EndOfStream;
        i += n;
    }
}

/// Write one frame to a stream: u32 LE length + payload, fully flushed before
/// returning. All write failures surface as error.WriteFailed (the daemon
/// closes the connection on it — EPIPE arrives as BrokenPipe because the
/// default Io installs a no-op SIGPIPE handler).
pub fn writeFrame(stream: std.Io.net.Stream, io: std.Io, bytes: []const u8) FrameError!void {
    if (bytes.len > max_frame) return error.FrameTooLarge;
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(bytes.len), .little);
    var buf: [8192]u8 = undefined;
    var w = stream.writer(io, &buf);
    w.interface.writeAll(&hdr) catch return error.WriteFailed;
    w.interface.writeAll(bytes) catch return error.WriteFailed;
    w.interface.flush() catch return error.WriteFailed;
}

// ---------------------------------------------------------------------------
// JSON escaping / encoding
// ---------------------------------------------------------------------------

/// Returns `s` with JSON-escaped content (no surrounding quotes). Handles
/// quote, backslash, \n \r \t and other control chars as \u00XX; passes all
/// well-formed UTF-8 through unchanged.
pub fn escapeJson(alloc: Allocator, s: []const u8) Allocator.Error![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.ensureTotalCapacity(alloc, s.len);
    for (s) |c| {
        switch (c) {
            '"' => try out.appendSlice(alloc, "\\\""),
            '\\' => try out.appendSlice(alloc, "\\\\"),
            '\n' => try out.appendSlice(alloc, "\\n"),
            '\r' => try out.appendSlice(alloc, "\\r"),
            '\t' => try out.appendSlice(alloc, "\\t"),
            else => {
                if (c < 0x20) {
                    var tmp: [6]u8 = undefined;
                    const hex = std.fmt.bufPrint(&tmp, "\\u{x:0>4}", .{c}) catch unreachable;
                    try out.appendSlice(alloc, hex);
                } else {
                    try out.append(alloc, c);
                }
            },
        }
    }
    return out.toOwnedSlice(alloc);
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

pub const Request = struct {
    op: []const u8,
    ch: ?[]const u8 = null,
    body: ?[]const u8 = null,
    ch_any: ?[]const []const u8 = null,
    timeout_ms: ?i64 = null,
    /// send only: fan out to every parked waiter and retain a copy.
    multi: ?bool = null,
    /// history only: return only messages with id strictly greater than this.
    after: ?u64 = null,
};

pub fn decodeRequest(alloc: Allocator, bytes: []const u8) !Request {
    return std.json.parseFromSliceLeaky(Request, alloc, bytes, .{ .ignore_unknown_fields = true });
}

pub fn encodeSend(alloc: Allocator, ch: []const u8, body: []const u8, multi: bool) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    const b = try escapeJson(alloc, body);
    defer alloc.free(b);
    if (multi) {
        return std.fmt.allocPrint(alloc, "{{\"op\":\"send\",\"ch\":\"{s}\",\"body\":\"{s}\",\"multi\":true}}", .{ c, b });
    }
    return std.fmt.allocPrint(alloc, "{{\"op\":\"send\",\"ch\":\"{s}\",\"body\":\"{s}\"}}", .{ c, b });
}

pub fn encodeRecv(alloc: Allocator, ch: []const u8, timeout_ms: ?i64) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    if (timeout_ms) |t| {
        return std.fmt.allocPrint(alloc, "{{\"op\":\"recv\",\"ch\":\"{s}\",\"timeout_ms\":{d}}}", .{ c, t });
    }
    return std.fmt.allocPrint(alloc, "{{\"op\":\"recv\",\"ch\":\"{s}\"}}", .{c});
}

/// recv with a catch-up cursor: replay the oldest transcript message with id >
/// `after`, else block for the next live send.
pub fn encodeRecvFrom(alloc: Allocator, ch: []const u8, after: u64, timeout_ms: ?i64) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    if (timeout_ms) |t| {
        return std.fmt.allocPrint(alloc, "{{\"op\":\"recv\",\"ch\":\"{s}\",\"after\":{d},\"timeout_ms\":{d}}}", .{ c, after, t });
    }
    return std.fmt.allocPrint(alloc, "{{\"op\":\"recv\",\"ch\":\"{s}\",\"after\":{d}}}", .{ c, after });
}

pub fn encodeRecvAny(alloc: Allocator, chans: []const []const u8, timeout_ms: ?i64) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(alloc);
    for (chans, 0..) |ch, i| {
        const c = try escapeJson(alloc, ch);
        defer alloc.free(c);
        if (i != 0) try list.append(alloc, ',');
        try list.append(alloc, '"');
        try list.appendSlice(alloc, c);
        try list.append(alloc, '"');
    }
    if (timeout_ms) |t| {
        return std.fmt.allocPrint(alloc, "{{\"op\":\"recv\",\"ch_any\":[{s}],\"timeout_ms\":{d}}}", .{ list.items, t });
    }
    return std.fmt.allocPrint(alloc, "{{\"op\":\"recv\",\"ch_any\":[{s}]}}", .{list.items});
}

pub fn encodePoll(alloc: Allocator, ch: []const u8) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    return std.fmt.allocPrint(alloc, "{{\"op\":\"poll\",\"ch\":\"{s}\"}}", .{c});
}

pub fn encodeClose(alloc: Allocator, ch: []const u8) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    return std.fmt.allocPrint(alloc, "{{\"op\":\"close\",\"ch\":\"{s}\"}}", .{c});
}

pub fn encodeHistory(alloc: Allocator, ch: []const u8, after: ?u64) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    if (after) |a| {
        return std.fmt.allocPrint(alloc, "{{\"op\":\"history\",\"ch\":\"{s}\",\"after\":{d}}}", .{ c, a });
    }
    return std.fmt.allocPrint(alloc, "{{\"op\":\"history\",\"ch\":\"{s}\"}}", .{c});
}

/// Ops with no extra fields: list, who, shutdown, ping.
pub fn encodeSimple(alloc: Allocator, op: []const u8) ![]u8 {
    const o = try escapeJson(alloc, op);
    defer alloc.free(o);
    return std.fmt.allocPrint(alloc, "{{\"op\":\"{s}\"}}", .{o});
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

pub const ChannelInfo = struct {
    ch: []const u8,
    queued: u64,
    parked: u64,
};

/// A single retained transcript message returned by `history`.
pub const HistoryMsg = struct {
    id: u64,
    ts: i64,
    body: []const u8,
};

pub const Response = struct {
    ok: bool,
    err: ?[]const u8 = null,
    ch: ?[]const u8 = null,
    body: ?[]const u8 = null,
    ts: ?i64 = null,
    id: ?u64 = null,
    queued: ?u64 = null,
    woken: ?u64 = null,
    empty: ?bool = null,
    count: ?u64 = null,
    pid: ?i64 = null,
    uptime_ms: ?u64 = null,
    sock: ?[]const u8 = null,
    channels: ?[]const ChannelInfo = null,
    history: ?[]const HistoryMsg = null,
};

pub fn decodeResponse(alloc: Allocator, bytes: []const u8) !Response {
    return std.json.parseFromSliceLeaky(Response, alloc, bytes, .{ .ignore_unknown_fields = true });
}

pub fn encodeErr(alloc: Allocator, msg: []const u8) ![]u8 {
    const m = try escapeJson(alloc, msg);
    defer alloc.free(m);
    return std.fmt.allocPrint(alloc, "{{\"ok\":false,\"err\":\"{s}\"}}", .{m});
}

pub fn encodeOk(alloc: Allocator) ![]u8 {
    return alloc.dupe(u8, "{\"ok\":true}");
}

/// recv success (also used when a parked waiter is woken by a send).
pub fn encodeRecvOk(alloc: Allocator, ch: []const u8, body: []const u8, ts_ms: i64, id: u64) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    const b = try escapeJson(alloc, body);
    defer alloc.free(b);
    return std.fmt.allocPrint(alloc, "{{\"ok\":true,\"ch\":\"{s}\",\"body\":\"{s}\",\"ts\":{d},\"id\":{d}}}", .{ c, b, ts_ms, id });
}

/// recv/poll found nothing queued (recv also uses this for a timeout expiry).
pub fn encodeEmptyOk(alloc: Allocator) ![]u8 {
    return alloc.dupe(u8, "{\"ok\":true,\"empty\":true}");
}

pub fn encodeSendOk(alloc: Allocator, id: u64, queued: usize, woken: usize) ![]u8 {
    return std.fmt.allocPrint(alloc, "{{\"ok\":true,\"id\":{d},\"queued\":{d},\"woken\":{d}}}", .{ id, queued, woken });
}

pub fn encodePollOk(alloc: Allocator, count: usize) ![]u8 {
    if (count == 0) return encodeEmptyOk(alloc);
    return std.fmt.allocPrint(alloc, "{{\"ok\":true,\"empty\":false,\"count\":{d}}}", .{count});
}

pub fn encodeListOk(alloc: Allocator, chans: []const ChannelInfo) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"ok\":true,\"channels\":[");
    for (chans, 0..) |ci, i| {
        const c = try escapeJson(alloc, ci.ch);
        defer alloc.free(c);
        if (i != 0) try out.append(alloc, ',');
        try out.print(alloc, "{{\"ch\":\"{s}\",\"queued\":{d},\"parked\":{d}}}", .{ c, ci.queued, ci.parked });
    }
    try out.appendSlice(alloc, "]}");
    return out.toOwnedSlice(alloc);
}

pub fn encodeHistoryOk(alloc: Allocator, ch: []const u8, msgs: []const HistoryMsg) ![]u8 {
    const c = try escapeJson(alloc, ch);
    defer alloc.free(c);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    try out.print(alloc, "{{\"ok\":true,\"ch\":\"{s}\",\"history\":[", .{c});
    for (msgs, 0..) |m, i| {
        const b = try escapeJson(alloc, m.body);
        defer alloc.free(b);
        if (i != 0) try out.append(alloc, ',');
        try out.print(alloc, "{{\"id\":{d},\"ts\":{d},\"body\":\"{s}\"}}", .{ m.id, m.ts, b });
    }
    try out.appendSlice(alloc, "]}");
    return out.toOwnedSlice(alloc);
}

pub fn encodeWhoOk(alloc: Allocator, pid: i64, uptime_ms: u64, sock: []const u8) ![]u8 {
    const s = try escapeJson(alloc, sock);
    defer alloc.free(s);
    return std.fmt.allocPrint(alloc, "{{\"ok\":true,\"pid\":{d},\"uptime_ms\":{d},\"sock\":\"{s}\"}}", .{ pid, uptime_ms, s });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "request decode roundtrips all fields" {
    const a = testing.allocator;
    const req_json = "{\"op\":\"send\",\"ch\":\"a b\",\"body\":\"line1\\nline2\"}";
    const req = try decodeRequest(a, req_json);
    try testing.expectEqualStrings("send", req.op);
    try testing.expectEqualStrings("a b", req.ch.?);
    try testing.expectEqualStrings("line1\nline2", req.body.?);

    const r2 = try decodeRequest(a, "{\"op\":\"recv\",\"ch\":\"x\",\"timeout_ms\":5000}");
    try testing.expectEqual(@as(i64, 5000), r2.timeout_ms.?);

    const r3 = try decodeRequest(a, "{\"op\":\"recv\",\"ch_any\":[\"p\",\"q\"],\"timeout_ms\":0}");
    try testing.expectEqual(@as(usize, 2), r3.ch_any.?.len);
    try testing.expectEqualStrings("q", r3.ch_any.?[1]);

    const r4 = try decodeRequest(a, "{\"op\":\"who\"}");
    try testing.expect(r4.ch == null);
}

test "encodeSend emits multi flag only when requested" {
    const a = testing.allocator;
    const single = try encodeSend(a, "ch", "b", false);
    defer a.free(single);
    try testing.expectEqualStrings("{\"op\":\"send\",\"ch\":\"ch\",\"body\":\"b\"}", single);

    const multi = try encodeSend(a, "ch", "b", true);
    defer a.free(multi);
    try testing.expectEqualStrings("{\"op\":\"send\",\"ch\":\"ch\",\"body\":\"b\",\"multi\":true}", multi);
    const req = try decodeRequest(a, multi);
    try testing.expect(req.multi.?);

    // Old clients (no multi field) decode as null -> single delivery.
    const old = try decodeRequest(a, single);
    try testing.expect(old.multi == null);
}

test "recv ch_any request is valid json with quoted names" {
    const a = testing.allocator;
    const req_bytes = try encodeRecvAny(a, &.{ "m", "q \"x\"" }, 5000);
    defer a.free(req_bytes);
    try testing.expectEqualStrings(
        "{\"op\":\"recv\",\"ch_any\":[\"m\",\"q \\\"x\\\"\"],\"timeout_ms\":5000}",
        req_bytes,
    );
    const req = try decodeRequest(a, req_bytes);
    try testing.expect(req.ch_any != null);
    try testing.expectEqual(@as(usize, 2), req.ch_any.?.len);
    try testing.expectEqualStrings("q \"x\"", req.ch_any.?[1]);
    try testing.expectEqual(@as(i64, 5000), req.timeout_ms.?);
}

test "response decode roundtrips" {
    const a = testing.allocator;
    const resp = try encodeRecvOk(a, "ch1", "hello \"world\"", 12345, 7);
    defer a.free(resp);
    const r = try decodeResponse(a, resp);
    try testing.expect(r.ok);
    try testing.expectEqualStrings("ch1", r.ch.?);
    try testing.expectEqualStrings("hello \"world\"", r.body.?);
    try testing.expectEqual(@as(i64, 12345), r.ts.?);
    try testing.expectEqual(@as(u64, 7), r.id.?);

    const e = try encodeErr(a, "boom \"x\"");
    defer a.free(e);
    const er = try decodeResponse(a, e);
    try testing.expect(!er.ok);
    try testing.expectEqualStrings("boom \"x\"", er.err.?);
}

test "recv --from encodes after cursor" {
    const a = testing.allocator;
    const req = try encodeRecvFrom(a, "h", 42, null);
    defer a.free(req);
    try testing.expectEqualStrings("{\"op\":\"recv\",\"ch\":\"h\",\"after\":42}", req);

    const r = try decodeRequest(a, req);
    try testing.expectEqualStrings("recv", r.op);
    try testing.expectEqual(@as(u64, 42), r.after.?);
}

test "history request encodes after only when given" {
    const a = testing.allocator;
    const plain = try encodeHistory(a, "h", null);
    defer a.free(plain);
    try testing.expectEqualStrings("{\"op\":\"history\",\"ch\":\"h\"}", plain);

    const with_after = try encodeHistory(a, "h", 42);
    defer a.free(with_after);
    try testing.expectEqualStrings("{\"op\":\"history\",\"ch\":\"h\",\"after\":42}", with_after);
    const req = try decodeRequest(a, with_after);
    try testing.expectEqual(@as(u64, 42), req.after.?);
}

test "history response encodes ordered messages and escapes bodies" {
    const a = testing.allocator;
    const msgs = [_]HistoryMsg{
        .{ .id = 1, .ts = 10, .body = "a" },
        .{ .id = 2, .ts = 20, .body = "line\n\"q\"" },
    };
    const resp = try encodeHistoryOk(a, "h", &msgs);
    defer a.free(resp);
    const r = try decodeResponse(a, resp);
    try testing.expect(r.ok);
    try testing.expectEqualStrings("h", r.ch.?);
    try testing.expectEqual(@as(usize, 2), r.history.?.len);
    try testing.expectEqual(@as(u64, 1), r.history.?[0].id);
    try testing.expectEqualStrings("a", r.history.?[0].body);
    try testing.expectEqual(@as(i64, 20), r.history.?[1].ts);
    try testing.expectEqualStrings("line\n\"q\"", r.history.?[1].body);
}

test "escapeJson escapes specials and control bytes" {
    const a = testing.allocator;
    const cases = [_]struct { in: []const u8, want: []const u8 }{
        .{ .in = "plain", .want = "plain" },
        .{ .in = "a\"b\\c", .want = "a\\\"b\\\\c" },
        .{ .in = "x\ny\tz\r", .want = "x\\ny\\tz\\r" },
        .{ .in = "\x01\x1f", .want = "\\u0001\\u001f" },
        .{ .in = "utf8-é✓", .want = "utf8-é✓" },
    };
    for (cases) |c| {
        const got = try escapeJson(a, c.in);
        defer a.free(got);
        try testing.expectEqualStrings(c.want, got);
    }
}

// Raw single-write helper over the raw linux write syscall (there is no
// std.posix.write in 0.16); used to feed frames into pipe fds for tests.
fn testPipeWrite(fd: std.posix.fd_t, bytes: []const u8) void {
    var i: usize = 0;
    while (i < bytes.len) {
        const r = std.os.linux.write(fd, bytes[i..].ptr, bytes[i..].len);
        if (std.os.linux.E.init(r) != .SUCCESS) continue;
        i += r;
    }
}

fn makePipe() ![2]std.posix.fd_t {
    return std.posix.pipe();
}

fn testStream(fd: std.posix.fd_t) std.Io.net.Stream {
    return .{ .socket = .{ .handle = fd, .address = .{ .ip4 = .loopback(0) } } };
}

test "readFrame reassembles byte-by-byte partial reads" {
    const a = testing.allocator;
    const p = try makePipe();
    defer std.posix.close(p[0]);
    defer std.posix.close(p[1]);

    const payload = "{\"op\":\"send\",\"ch\":\"ch\",\"body\":\"partial read test payload\"}";
    var header: [4]u8 = undefined;
    std.mem.writeInt(u32, &header, @intCast(payload.len), .little);
    for (header) |b| testPipeWrite(p[1], &.{b});
    for (payload) |b| testPipeWrite(p[1], &.{b});

    const frame = try readFrame(p[0], a);
    defer a.free(frame);
    try testing.expectEqualStrings(payload, frame);
}

test "readFrame rejects oversize frame header" {
    const a = testing.allocator;
    const p = try makePipe();
    defer std.posix.close(p[0]);
    defer std.posix.close(p[1]);

    var header: [4]u8 = undefined;
    std.mem.writeInt(u32, &header, max_frame + 1, .little);
    testPipeWrite(p[1], &header);
    try testing.expectError(error.FrameTooLarge, readFrame(p[0], a));
}

test "readFrame reports EOF on truncated body" {
    const a = testing.allocator;
    const p = try makePipe();
    defer std.posix.close(p[0]);
    defer std.posix.close(p[1]);

    var header: [4]u8 = undefined;
    std.mem.writeInt(u32, &header, 100, .little);
    testPipeWrite(p[1], &header);
    testPipeWrite(p[1], "only 16");
    std.posix.close(p[1]);
    try testing.expectError(error.EndOfStream, readFrame(p[0], a));
}

test "writeFrame then readFrame roundtrip through a pipe" {
    const a = testing.allocator;
    const p = try makePipe();
    defer std.posix.close(p[0]);
    defer std.posix.close(p[1]);

    const payload = "{\"ok\":true,\"body\":\"roundtrip\"}";
    try writeFrame(testStream(p[1]), testing.io, payload);
    const frame = try readFrame(p[0], a);
    defer a.free(frame);
    try testing.expectEqualStrings(payload, frame);
}

test "writeFrame rejects oversize payload" {
    const p = try makePipe();
    defer std.posix.close(p[0]);
    defer std.posix.close(p[1]);
    const big = try testing.allocator.alloc(u8, max_frame + 1);
    defer testing.allocator.free(big);
    try testing.expectError(error.FrameTooLarge, writeFrame(testStream(p[1]), testing.io, big));
}

test "unix socket path length guard at 108 bytes" {
    var path_buf: [130]u8 = undefined;
    @memset(&path_buf, 'a');
    const ok = try std.Io.net.UnixAddress.init(path_buf[0..unix_path_max]);
    try testing.expect(ok.path.len == unix_path_max);
    try testing.expectError(error.NameTooLong, std.Io.net.UnixAddress.init(path_buf[0 .. unix_path_max + 1]));
}
