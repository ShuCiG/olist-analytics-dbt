#!/usr/bin/env bash
# `terraform init` that survives a flaky provider download (#258, D106).
# Retries only on network/registry errors (the 504 from GitHub release assets,
# "could not query provider registry"); any other init error (a checksum
# mismatch, a bad config, a missing lock entry) fails on the first attempt.
# Usage: bash terraform_init.sh <terraform init args...>
set -uo pipefail

log="${RUNNER_TEMP:-/tmp}/terraform-init.log"
transient='could not query provider registry|bad response code: 5[0-9][0-9]|i/o timeout|TLS handshake timeout|connection reset by peer|unexpected EOF|context deadline exceeded'

for attempt in 1 2 3; do
  if terraform init "$@" 2>&1 | tee "$log"; then
    exit 0
  fi
  if grep -q 'checksum' "$log" || ! grep -qE "$transient" "$log"; then
    echo "::error::terraform init failed (not a download error), no retry"
    exit 1
  fi
  if [ "$attempt" -lt 3 ]; then
    echo "::warning::terraform init: provider download error, retry $((attempt + 1)) of 3 in $((attempt * 20)) s"
    sleep $((attempt * 20))
  fi
done
echo "::error::terraform init: provider download failed 3 times"
exit 1
