#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/../scripts/deploy-proxmox.sh"
LOCAL_TMP=$(mktemp -d)
trap 'rm -rf "$LOCAL_TMP"' EXIT

HA_URL=http://ha:8123
TOKEN_FILE="$LOCAL_TMP/missing-token"
HA_TOKEN=test-token
CH_URL=http://clickhouse:8123
CH_PASSWORD='a"b\c'
RETENTION_DAYS=365
BUFFER_CAPACITY=1000
BUFFER_MAX_BYTES=33554432
INCLUDE_ATTRIBUTES=false
EXCLUDE_ENTITIES=sensor.a,sensor.b
ATTRIBUTES_MAX_BYTES=65536
LOG_FORMAT=json
write_files
for setting in 'RETENTION_DAYS="365"' 'BUFFER_CAPACITY="1000"' \
    'BUFFER_MAX_BYTES="33554432"' 'INCLUDE_ATTRIBUTES="false"' \
    'EXCLUDE_ENTITIES="sensor.a,sensor.b"' 'ATTRIBUTES_MAX_BYTES="65536"' \
    'LOG_FORMAT="json"' 'CLICKHOUSE_PASSWORD="a\"b\\c"'; do
    grep -Fxq "$setting" "$LOCAL_TMP/ha-ingestor.env"
done
[ "$(cat "$LOCAL_TMP/ha-ingestor.token")" = test-token ]
unset RETENTION_DAYS
write_files
! grep -q '^RETENTION_DAYS=' "$LOCAL_TMP/ha-ingestor.env"

pveam() {
    case "$1" in
        update) return ;;
        available) printf '%s\n' \
            'system debian-12-standard_12.12-1_amd64.tar.zst' \
            'system debian-13-standard_13.6-1_arm64.tar.zst' \
            'system debian-13-standard_13.5-1_amd64.tar.zst' \
            'system debian-13-standard_13.6-1_amd64.tar.zst' ;;
        *) return 1 ;;
    esac
}
run() { eval "$1"; }
ARCH=amd64
TEMPLATE=
detect_template
[ "$TEMPLATE" = debian-13-standard_13.6-1_amd64.tar.zst ]

# Bad forwarded settings must fail before SSH or deployment.
for setting in BUFFER_MAX_BYTES=0 RETENTION_DAYS=oops INCLUDE_ATTRIBUTES=oops; do
    if env "$setting" HA_TOKEN=test-token bash "$SCRIPT_DIR/deploy-proxmox.sh" \
        --pve root@example --ha-url http://ha:8123 --clickhouse-url http://ch:8123 \
        --dry-run >"$LOCAL_TMP/output" 2>&1; then
        printf 'Unexpectedly accepted %s\n' "$setting" >&2
        exit 1
    fi
done
printf 'Deployment script checks passed\n'
