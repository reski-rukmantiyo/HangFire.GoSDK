#!/usr/bin/env bash
# Usage: scripts/sqlserver.sh up|down|status
set -euo pipefail

PROFILE="${COLIMA_PROFILE:-hangfire}"
NAME="${MSSQL_CONTAINER:-hangfire-mssql}"
# 2022-latest (MSSQL_IMAGE=mcr.microsoft.com/mssql/server:2022-latest MSSQL_PLATFORM=linux/amd64) crashes under colima qemu (no rosetta mount); azure-sql-edge runs natively on arm64.
IMAGE="${MSSQL_IMAGE:-mcr.microsoft.com/azure-sql-edge:latest}"
PLATFORM="${MSSQL_PLATFORM:-linux/arm64}"
PORT="${MSSQL_PORT:-1433}"
export HANGFIRE_SA_PASSWORD="${HANGFIRE_SA_PASSWORD:-Hangfire_Dev_Passw0rd!}"

nerdctl() { colima nerdctl -p "$PROFILE" -- "$@"; }

# sqlcmd exists in the 2022 image only; azure-sql-edge ships none, so fall back to the ready log line plus a host TCP check.
sql_ready() {
  if nerdctl exec "$NAME" sh -c 'ls /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd' >/dev/null 2>&1; then
    nerdctl exec -e "P=$HANGFIRE_SA_PASSWORD" "$NAME" bash -c '
      for s in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
        [ -x "$s" ] && exec "$s" -C -S localhost -U sa -P "$P" -Q "SELECT 1" -b
      done' >/dev/null 2>&1
  else
    local logs; logs="$(nerdctl logs "$NAME" 2>&1)"
    grep -q "ready for client connections" <<<"$logs" && nc -z localhost "$PORT"
  fi
}

case "${1:-}" in
  up)
    if ! nerdctl ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
      nerdctl run -d --platform "$PLATFORM" --name "$NAME" \
        -e ACCEPT_EULA=Y -e "MSSQL_SA_PASSWORD=$HANGFIRE_SA_PASSWORD" \
        --network host "$IMAGE" >/dev/null
    elif ! nerdctl ps --format '{{.Names}}' | grep -qx "$NAME"; then
      nerdctl start "$NAME" >/dev/null
    fi
    for _ in $(seq 1 90); do
      if sql_ready; then echo "sqlserver ready on localhost:$PORT"; exit 0; fi
      sleep 2
    done
    echo "timed out waiting for login" >&2; nerdctl logs --tail 30 "$NAME" >&2 || true; exit 1 ;;
  down)
    nerdctl rm -f "$NAME" >/dev/null 2>&1 || true; echo "removed $NAME" ;;
  status)
    nerdctl ps -a --filter "name=$NAME" --format '{{.Names}} {{.Status}}'
    if sql_ready; then echo "login: ok"; else echo "login: failed"; exit 1; fi
    nc -z localhost "$PORT" && echo "host port $PORT: open" || { echo "host port $PORT: closed"; exit 1; } ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac
