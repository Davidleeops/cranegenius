#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-.}"
cd "$ROOT"
rg -n --hidden -g "!/.git" -g "!data/**" -g "!assets/images/**" \
  -e "sk-ant-api03-[A-Za-z0-9_-]+" \
  -e "sk-proj-[A-Za-z0-9_-]+" \
  -e "AIza[0-9A-Za-z_-]+" \
  -e "AKIA[0-9A-Z]{16}" \
  -e "-----BEGIN (RSA|EC|OPENSSH|DSA)? ?PRIVATE KEY-----" \
  || true
