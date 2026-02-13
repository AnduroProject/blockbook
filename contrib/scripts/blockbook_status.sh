#!/usr/bin/env bash
set -euo pipefail

die() { echo "error: $1" >&2; exit 1; }
[[ $# -ge 1 ]] || die "missing coin argument. usage: blockbook_status.sh <coin> [hostname]"
coin="$1"
if [[ -n "${2-}" ]]; then
  host="$2"
else
  host="localhost"
fi

var="B_PORT_PUBLIC_${coin}"
port="${!var-}"
[[ -n "$port" ]] || die "environment variable ${var} is not set"
command -v curl >/dev/null 2>&1 || die "curl is not installed"
command -v jq >/dev/null 2>&1 || die "jq is not installed"

curl -skv "https://${host}:${port}/api/status" | jq