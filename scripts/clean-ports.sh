#!/usr/bin/env bash
#
# For demonstration purposes only.
#
# Sweep diab host ports for leftover holders after compose-down.
#
# Behavior:
#   - Colima SSH multiplexer holding a diab port → `colima stop` (idempotent).
#     Triggered when the user previously deployed on Colima, switched runtime
#     to Docker Desktop, but Colima auto-resumed its port-forwards in the
#     background. This is the bug that surfaced on 2026-05-18 with :8201.
#   - Docker Desktop / vpnkit holding a port → printed as guidance only.
#     We don't auto-restart Docker Desktop; too invasive.
#   - Any other process → printed and reported as a foreign conflict.
#     Killed only if FORCE_KILL_PORTS=1 is set in the environment.
#
# Exit codes:
#   0  all swept ports clean (or only Colima held them, and we stopped it)
#   1  one or more ports still held by a foreign process
#
# Used by Makefile `clean` target and engine/agent/app.py `api_exit`.

set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FORCE_KILL="${FORCE_KILL_PORTS:-0}"

PORTS=$(python3 "$PROJECT_ROOT/scripts/list-stack-ports.py" 2>/dev/null)
if [ -z "$PORTS" ]; then
    echo "[clean-ports] No ports discovered from compose files; nothing to sweep."
    exit 0
fi

# Return codes from check_port:
#   0  port is free
#  10  Colima holds it (caller should stop Colima)
#  11  killed (FORCE_KILL_PORTS=1)
#   1  foreign holder remains
check_port() {
    local port=$1
    local out
    out=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | tail -n +2)
    [ -z "$out" ] && return 0

    echo "  :$port"
    echo "$out" | sed 's/^/      /'

    if echo "$out" | grep -q 'colima/ssh.sock'; then
        return 10
    fi

    local cmd
    cmd=$(echo "$out" | awk 'NR==1 {print $1}')
    if [ "$cmd" = "com.docke" ] || [ "$cmd" = "vpnkit" ]; then
        echo "      → Docker Desktop / vpnkit holding the port. Not auto-restarted."
        echo "        If this persists, restart Docker Desktop manually."
        return 1
    fi

    if [ "$FORCE_KILL" = "1" ]; then
        local pid
        pid=$(echo "$out" | awk 'NR==1 {print $2}')
        echo "      → FORCE_KILL_PORTS=1: killing PID $pid"
        kill -9 "$pid" 2>/dev/null || true
        return 11
    fi
    return 1
}

echo "Sweeping diab host ports for leftover holders..."

stopped_colima=0
foreign=()
killed=0
for port in $PORTS; do
    check_port "$port"
    case $? in
        10) stopped_colima=1 ;;
         1) foreign+=(":$port") ;;
        11) killed=$((killed + 1)) ;;
    esac
done

if [ "$stopped_colima" = "1" ]; then
    echo ""
    echo "Stopping Colima VM (it was holding one or more diab ports)..."
    colima stop >/dev/null 2>&1 || true
    sleep 1
    foreign=()
    for port in $PORTS; do
        if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            holder=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $1}')
            if [ "$holder" != "ssh" ]; then
                foreign+=(":$port($holder)")
            fi
        fi
    done
fi

if [ "$killed" -gt 0 ]; then
    echo ""
    echo "FORCE_KILL_PORTS=1 killed $killed process(es)."
fi

if [ ${#foreign[@]} -gt 0 ]; then
    echo ""
    echo "WARNING: ${#foreign[@]} port(s) still held by foreign processes:"
    printf '  %s\n' "${foreign[@]}"
    echo ""
    echo "        Re-run with: FORCE_KILL_PORTS=1 make clean"
    echo "        (or stop the holders manually if they are intentional)"
    exit 1
fi

echo "All swept diab ports are free."
exit 0
