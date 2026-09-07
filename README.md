# fx-agent-bus

A real-time message bus for agents. One daemon (autostarted on first use) holds
per-channel FIFO queues over an AF_UNIX stream socket; CLI clients connect,
exchange one JSON frame, and exit.

It is the **live, in-flight** coordination channel for parallel agents — the
complement to the durable knowledge-graph memory in
[`fx-agent-memory`](../fx-agent-memory). The graph records what happened; the
bus lets agents talk while it happens.

The bus is **transient**: it dies on shutdown/reboot and its queues are
best-effort, so treat it as coordination-only, never as the record of truth.

## Build

Requires Zig 0.16.

```sh
zig build            # builds zig-out/bin/fx-agent-bus
zig build test       # unit tests
tests/integration.sh # end-to-end suite (needs zig-out/bin/fx-agent-bus)
```

The build forces the portable x86_64 baseline CPU (SSE2 only) so the binary
runs on any host and can never accidentally pick up AVX/AVX2/AVX512.

## Usage

```sh
fx-agent-bus daemon                     # run the bus daemon in the foreground
fx-agent-bus start                      # autostart the daemon, print its pid
fx-agent-bus send <ch> <body...>        # append a message to channel <ch>
fx-agent-bus send --multi <ch> <body...> # wake every parked waiter (one copy
                                         # each) AND queue a copy for later
                                         # consumers; a plain send fans out the
                                         # same way when >1 waiter is parked
fx-agent-bus recv <ch> [--timeout SEC]  # pop the oldest message (waits if empty)
fx-agent-bus recv <ch> --from ID [--timeout SEC]  # catch up: replay oldest
                                         # message newer than ID, else wait
fx-agent-bus recv --any <ch>... [--timeout SEC]   # pop from the first of several
fx-agent-bus poll <ch>                  # report queue depth without consuming
fx-agent-bus history <ch> [--after ID]  # print retained messages after id
fx-agent-bus list                       # list channels (queued/parked counts)
fx-agent-bus close <ch>                 # drop a channel and its queued messages
fx-agent-bus who                        # daemon pid, uptime and socket path
fx-agent-bus shutdown                   # stop the daemon (queues not persisted)
fx-agent-bus ping                       # liveness check
```

### Environment

| Variable                     | Meaning                                                            |
| ---------------------------- | ------------------------------------------------------------------ |
| `FX_AGENT_BUS_SOCK`          | overrides the default `~/.config/hax/bus.sock` socket path          |
| `FX_AGENT_BUS_RECV_TIMEOUT`  | default `recv` wait when `--timeout` omitted (seconds, default 60)  |
| `FX_AGENT_BUS_MAX_WAIT`      | daemon cap on any parked `recv` (seconds, default 300; `0` disables)|

## Design notes

- **Concurrency:** a single-threaded `poll()` loop — no threads. Every
  connection carries at most one request. The only long-lived connections are
  parked `recv` waiters, kept in the poll set solely to observe EOF and
  deadlines.
- **Wire protocol:** each frame is a u32 LE length (capped at 1 MiB) followed by
  UTF-8 JSON — `{"op":..., ...}` requests, `{"ok":true,...}` /
  `{"ok":false,"err":...}` replies. JSON is hand-rolled; parsing goes through
  `std.json.parseFromSliceLeaky`.
- **Catch-up:** each channel retains a bounded transcript (`history`, default
  cap 100) of recent messages *independent of* the live FIFO, so a late joiner
  re-reads via `history <ch> --after <id>` even after a live consumer drained
  the queue.
- Stale-socket recovery hits `ECONNREFUSED` on an expected path; stack tracing
  is disabled for that case.

## License

Part of the `fixpoint-linux` Linux-distro project. See the org-level license.
