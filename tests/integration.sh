#!/usr/bin/env bash
# Integration tests for fx-agent-bus. Requires ./zig-out/bin/fx-agent-bus.
# Every invocation uses an isolated temp socket via FX_AGENT_BUS_SOCK.
set -euo pipefail

BUS="$(cd "$(dirname "$0")/.." && pwd)/zig-out/bin/fx-agent-bus"
[ -x "$BUS" ] || { echo "FAIL: $BUS not built (run zig build)"; exit 1; }

TMP="$(mktemp -d /tmp/fx-agent-bus-test.XXXXXX)"
SOCK="$TMP/bus.sock"
export FX_AGENT_BUS_SOCK="$SOCK"
DPID_FILE="$TMP/daemon.pid"

cleanup() {
    if [ -f "$DPID_FILE" ]; then
        kill -9 "$(cat "$DPID_FILE")" 2>/dev/null || true
    fi
    rm -rf "$TMP"
}
trap cleanup EXIT

capture_daemon_pid() {
    "$BUS" who | awk '{print $2}' > "$DPID_FILE"
}

fail() { echo "FAIL: $*"; exit 1; }

echo "== 1. cold autostart: first client spawns the daemon =="
rm -f "$SOCK"
OUT="$("$BUS" send alpha hello)"
echo "$OUT" | grep -Eq '^id [0-9]+ delivered [0-9]+$' || fail "cold autostart send: '$OUT'"
capture_daemon_pid

echo "== 2. recv parks, send from another process wakes it =="
( "$BUS" recv beta --timeout 5 > "$TMP/recv.out" ) & RG=$!
sleep 0.3
"$BUS" send beta "cross body" > /dev/null
wait "$RG" || fail "parked recv exited nonzero"
grep -qx 'cross body' "$TMP/recv.out" || fail "recv body: '$(cat "$TMP/recv.out")'"

echo "== 3. send-then-recv keeps FIFO order across 3 messages =="
"$BUS" send fifo one > /dev/null
"$BUS" send fifo two > /dev/null
"$BUS" send fifo three > /dev/null
for want in one two three; do
    got="$("$BUS" recv fifo --timeout 5)"
    [ "$got" = "$want" ] || fail "fifo order: want '$want' got '$got'"
done

echo "== 4. --any recv across two channels =="
( "$BUS" recv --any any1 any2 --timeout 5 > "$TMP/any.out" ) & AG=$!
sleep 0.3
"$BUS" send any2 "via-any2" > /dev/null
wait "$AG" || fail "--any recv exited nonzero"
grep -qx 'via-any2' "$TMP/any.out" || fail "--any body: '$(cat "$TMP/any.out")'"

echo "== 5. poll: empty vs nonempty, non-destructive =="
"$BUS" send pq "peeked" > /dev/null
OUT="$("$BUS" poll pq)"
echo "$OUT" | grep -q 'queued' || fail "poll nonempty: '$OUT'"
"$BUS" recv pq --timeout 5 > /dev/null     # consume it
OUT="$("$BUS" poll pq)"
[ "$OUT" = "empty" ] || fail "poll after consume: '$OUT'"
OUT="$("$BUS" poll never-created)"
[ "$OUT" = "empty" ] || fail "poll unknown channel: '$OUT'"

echo "== 6. close drops the queue =="
"$BUS" send cq "droppable" > /dev/null
"$BUS" send cq "droppable2" > /dev/null
"$BUS" close cq > /dev/null
if "$BUS" recv cq --timeout 0 > "$TMP/cq.out" 2>/dev/null; then
    fail "recv after close got a message: '$(cat "$TMP/cq.out")'"
fi
"$BUS" close cq > /dev/null 2>&1 && true   # closing again recreates+empties: fine either way

echo "== 7. who and list =="
OUT="$("$BUS" who)"
echo "$OUT" | grep -Eq '^pid [0-9]+ uptime_ms [0-9]+ sock '"$(echo "$SOCK" | sed 's/[][\\.*^$]/\\&/g')"'$' \
    || fail "who output: '$OUT'"
"$BUS" send lst b > /dev/null
"$BUS" send lst a > /dev/null
"$BUS" send other x > /dev/null
OUT="$("$BUS" list)"
echo "$OUT" | grep -qx 'lst queued=2 parked=0' || fail "list lst: '$OUT'"
echo "$OUT" | grep -qx 'other queued=1 parked=0' || fail "list other: '$OUT'"

echo "== 8. 512KiB body roundtrip via stdin (partial IO smoke) =="
python3 -c 'import sys; sys.stdout.write("A"*524288)' > "$TMP/big"
"$BUS" send big - < "$TMP/big" > /dev/null
"$BUS" recv big --timeout 5 > "$TMP/big.out"
SIZE=$(wc -c < "$TMP/big.out")
[ "$SIZE" -eq 524289 ] || fail "big body size: want 524289 got $SIZE"  # body + trailing \n
grep -q 'BBBB' "$TMP/big.out" && fail "big body corrupted"

echo "== 9. parked waiter timeout expiry =="
T0=$(date +%s)
if "$BUS" recv slowch --timeout 1 > /dev/null 2>&1; then
    fail "recv with empty queue should exit nonzero"
fi
T1=$(date +%s)
[ $((T1 - T0)) -le 3 ] || fail "recv --timeout 1 took $((T1 - T0))s"

echo "== 10. stale socket recovery (daemon kill -9) =="
OLDPID=$(cat "$DPID_FILE")
kill -9 "$OLDPID"
sleep 0.2
[ -e "$SOCK" ] || fail "expected stale socket file to remain after kill -9"
"$BUS" send recover "still alive" > /dev/null
capture_daemon_pid
NEWPID=$(cat "$DPID_FILE")
[ "$NEWPID" != "$OLDPID" ] || fail "daemon did not restart (same pid $NEWPID)"
OUT="$("$BUS" recv recover --timeout 5)"
[ "$OUT" = "still alive" ] || fail "recovered recv: '$OUT'"

echo "== 11. --multi fans out to all parked waiters and retains a copy =="
( "$BUS" recv mc --timeout 5 > "$TMP/m1.out" ) & M1=$!
( "$BUS" recv mc --timeout 5 > "$TMP/m2.out" ) & M2=$!
sleep 0.5
OUT="$("$BUS" send --multi mc "broadcast hello")"
echo "$OUT" | grep -Eq '^id [0-9]+ delivered 2$' || fail "multi send output: '$OUT'"
wait "$M1" || fail "multi waiter 1 exited nonzero"
wait "$M2" || fail "multi waiter 2 exited nonzero"
grep -qx 'broadcast hello' "$TMP/m1.out" || fail "multi waiter 1 body: '$(cat "$TMP/m1.out")'"
grep -qx 'broadcast hello' "$TMP/m2.out" || fail "multi waiter 2 body: '$(cat "$TMP/m2.out")'"
OUT="$("$BUS" poll mc)"
echo "$OUT" | grep -q 'queued' || fail "multi retained copy missing: '$OUT'"
OUT="$("$BUS" recv mc --timeout 5)"
[ "$OUT" = "broadcast hello" ] || fail "multi retained recv: '$OUT'"
OUT="$("$BUS" poll mc)"
[ "$OUT" = "empty" ] || fail "multi queue not drained: '$OUT'"

echo "== 12. history/--from: transcript catch-up survives consumption, cursor replays in order =="
"$BUS" send hist a > /dev/null
"$BUS" send hist b > /dev/null
"$BUS" send hist c > /dev/null
# Consume the live messages; they must remain on the transcript.
[ "$("$BUS" recv hist --timeout 5)" = "a" ] || fail "consume a"
[ "$("$BUS" recv hist --timeout 5)" = "b" ] || fail "consume b"
HOUT="$("$BUS" history hist)"
IDA=$(echo "$HOUT" | awk '/ a$/{print $2; exit}')   # id of a
IDB=$(echo "$HOUT" | awk '/ b$/{print $2; exit}')   # id of b
IDC=$(echo "$HOUT" | awk '/ c$/{print $2; exit}')   # id of c
[ -n "$IDA" ] && [ -n "$IDB" ] && [ -n "$IDC" ] || fail "history parse: '$HOUT'"
echo "$HOUT" | grep -qx "id $IDA ts [0-9]* a" || fail "history lacks a: '$HOUT'"
echo "$HOUT" | grep -qx "id $IDC ts [0-9]* c" || fail "history lacks c: '$HOUT'"
# --from IDA replays the oldest message newer than a -> b, then --from IDB -> c.
[ "$("$BUS" recv hist --from "$IDA" --timeout 5)" = "b" ] || fail "--from IDA -> b"
[ "$("$BUS" recv hist --from "$IDB" --timeout 5)" = "c" ] || fail "--from IDB -> c"
# Caught up at IDC: must block for a NEW send, not re-deliver queued a/b/c.
( "$BUS" recv hist --from "$IDC" --timeout 5 > "$TMP/from.out" ) & FG=$!
sleep 0.4
"$BUS" send hist d > /dev/null
wait "$FG" || fail "--from caught-up waiter exited nonzero"
grep -qx 'd' "$TMP/from.out" || fail "--from live got: '$(cat "$TMP/from.out")'"
rm -f "$TMP/from.out"

echo "== 13. shutdown unlinks the socket and stops the daemon =="
OLDPID=$(cat "$DPID_FILE")
"$BUS" shutdown > /dev/null
sleep 0.3
if [ -e "$SOCK" ]; then fail "socket file still exists after shutdown"; fi
if kill -0 "$OLDPID" 2> /dev/null; then fail "daemon pid $OLDPID still alive after shutdown"; fi
rm -f "$DPID_FILE"   # exited gracefully; nothing left to kill

echo
echo "ALL INTEGRATION TESTS PASSED"
