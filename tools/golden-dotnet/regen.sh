#!/usr/bin/env bash
# Regenerates hangfire/testdata/golden.json from the real Hangfire.Core package.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
mkdir -p "$root/hangfire/testdata"
dotnet run --project "$here/GoldenDotnet.csproj" -c Release -- "$root/hangfire/testdata/golden.json"
