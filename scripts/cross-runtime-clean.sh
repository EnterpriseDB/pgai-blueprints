#!/usr/bin/env bash
#
# For demonstration purposes only.
#
# Tear down diab compose projects on the *inactive* laptop runtime
# (Docker Desktop ↔ Colima). This is the cross-infra switching support: after
# a user picks a different deploy target, leftovers from the previous target
# must not block the new one.
#
# Idempotent — safe to call when only one runtime is present.
#
# Strategy:
#   - Detect the active Docker context (`docker context show`).
#   - If on Docker Desktop and Colima is also running → tear down on Colima too,
#     then stop the Colima VM.
#   - If on Colima and Docker Desktop is also running → tear down on Docker
#     Desktop too (do NOT stop Docker Desktop — restart of the .app is invasive).
#   - We do not modify the user's active context — we set DOCKER_CONTEXT just
#     for the duration of the inactive-runtime cleanup, then unset it.

set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

current_context=$(docker context show 2>/dev/null || echo "default")
echo "Active Docker context: $current_context"

# Probe whether Colima is running. Returns 0 if running.
is_colima_running() {
    command -v colima >/dev/null 2>&1 || return 1
    colima status >/dev/null 2>&1
}

# Probe whether a Docker context is reachable (daemon up + responding).
is_context_reachable() {
    DOCKER_CONTEXT="$1" docker info >/dev/null 2>&1
}

inactive_context=""
case "$current_context" in
    desktop-linux|default)
        is_colima_running && inactive_context="colima"
        ;;
    colima)
        # Check Docker Desktop is reachable on its socket
        if docker context inspect desktop-linux >/dev/null 2>&1 && \
           is_context_reachable desktop-linux; then
            inactive_context="desktop-linux"
        fi
        ;;
esac

if [ -z "$inactive_context" ]; then
    echo "No inactive runtime needs cleanup."
    exit 0
fi

echo "Inactive runtime detected: $inactive_context. Cleaning diab containers there too..."

export DOCKER_CONTEXT="$inactive_context"

cleaned_any=0
for dir in "$PROJECT_ROOT"/stacks/*/ "$PROJECT_ROOT"/plugins/*/; do
    [ -f "$dir/docker-compose.yaml" ] || continue
    base=$(basename "$dir")
    [ "$base" = "_template" ] && continue
    has=$(cd "$dir" && docker compose ps -aq 2>/dev/null | wc -l | tr -d ' ')
    if [ "$has" != "0" ]; then
        echo "  ($inactive_context) cleaning $base ($has containers)..."
        cd "$dir"
        PROFS=$(docker compose config --profiles 2>/dev/null | awk '{printf " --profile %s", $0}')
        eval "docker compose $PROFS kill" 2>/dev/null || true
        eval "docker compose $PROFS down -v --remove-orphans -t 1" 2>/dev/null || true
        cleaned_any=1
    fi
done

# Drop orphans on the inactive runtime too. Mirror prefix list from Makefile/api_exit.
PREFIXES=(rta- lab- cb- cba- bfd- uai- bench- cdc-rw- eapi-rw- kafka-rw- wh-rw- dbox- sovereign- tpl- pg-expense- diab-toolbox-)
for prefix in "${PREFIXES[@]}"; do
    ids=$(docker ps -aq --filter "name=$prefix" 2>/dev/null)
    if [ -n "$ids" ]; then
        echo "  ($inactive_context) removing $prefix orphans..."
        docker rm -f $ids >/dev/null 2>&1 || true
        cleaned_any=1
    fi
done

unset DOCKER_CONTEXT

if [ "$inactive_context" = "colima" ]; then
    echo "Stopping Colima VM (no longer needed by the active runtime)..."
    colima stop >/dev/null 2>&1 || true
fi

if [ "$cleaned_any" = "0" ]; then
    echo "No diab artefacts on $inactive_context."
else
    echo "Cross-runtime cleanup complete."
fi
