#!/usr/bin/env bash
#
# Deploy ha-ch-ingestor to Proxmox VE using the lean path:
# a static binary in an unprivileged Debian LXC, supervised by systemd.
#
# Run this from your workstation (the checkout). It uses local Docker to build
# the linux/amd64 (or linux/arm64) binary, then SSH + pct to create or update
# the container. No Docker is installed on the Proxmox host or in the LXC.
#
# Requirements:
#   * SSH access to a Proxmox node as root, or with passwordless sudo
#   * Docker locally (unless --binary is used)
#   * Internet access on the node for the Debian template download
#
# See --help for all options.

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)

# ---------------------------------------------------------------------------
# Defaults (override with flags or environment variables)
# ---------------------------------------------------------------------------
PVE=${PVE_HOST:-}
HA_URL=${HA_URL:-}
HA_WS_URL=${HA_WS_URL:-}
TOKEN_FILE=${HA_TOKEN_FILE:-"$REPO_DIR/../ha_token.txt"}
CH_URL=${CLICKHOUSE_URL:-}
CH_USER=${CLICKHOUSE_USER:-default}
CH_PASSWORD=${CLICKHOUSE_PASSWORD:-}
CH_PASSWORD_FILE=${CLICKHOUSE_PASSWORD_FILE:-}
CH_DB=${CLICKHOUSE_DATABASE:-homeassistant}
CH_TABLE=${CLICKHOUSE_TABLE:-states}
BATCH_SIZE=${BATCH_SIZE:-500}
FLUSH_INTERVAL=${FLUSH_INTERVAL:-5}
RUST_LOG_VALUE=${RUST_LOG:-info}
CREATE_SCHEMA=${CREATE_SCHEMA:-true}

CT_HOSTNAME=${CT_HOSTNAME:-ha-ch-ingestor}
CTID=${CTID:-}
STORAGE=${STORAGE:-}
TPL_STORAGE=${TPL_STORAGE:-}
TEMPLATE=${TEMPLATE:-}
BRIDGE=${BRIDGE:-}
IP=${IP:-dhcp}
GATEWAY=${GATEWAY:-}
NAMESERVER=${NAMESERVER:-}
CORES=${CORES:-1}
MEMORY=${MEMORY:-512}
SWAP=${SWAP:-512}
DISK=${DISK:-4}
ARCH=${ARCH:-}
BINARY=${BINARY:-}
IMAGE=${IMAGE:-ha-ch-ingestor:deploy}
DRY_RUN=0

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
step() { printf '\n==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }
warn() { printf 'warning: %s\n' "$*" >&2; }

usage() {
    cat <<'EOF'
Deploy ha-ch-ingestor to Proxmox VE (lean path: static binary in a Debian LXC).

Usage:
  scripts/deploy-proxmox.sh --pve root@PVE --ha-url URL --clickhouse-url URL [options]

Required:
  --pve USER@HOST              SSH target of a Proxmox node (root or passwordless sudo)
  --ha-url URL                 Home Assistant base URL, e.g. http://192.168.1.10:8123
      or --ha-ws-url URL       Full WebSocket URL, e.g. ws://192.168.1.10:8123/api/websocket
  --clickhouse-url URL         ClickHouse HTTP URL, e.g. http://192.168.1.20:8123

Home Assistant token:
  --ha-token-file PATH         Token file (default: ../ha_token.txt next to this repo)
      or HA_TOKEN env var      Used when the token file does not exist

ClickHouse credentials:
  --clickhouse-user NAME       Default: default
  --clickhouse-password PASS   Prefer --clickhouse-password-file or CLICKHOUSE_PASSWORD
  --clickhouse-password-file PATH
  --clickhouse-database NAME   Default: homeassistant
  --clickhouse-table NAME      Default: states (use a new name for an old Python table)
  --no-create-schema           Do not create the database/table at startup

Container:
  --ctid ID                    Target an existing ID (otherwise found by hostname)
  --hostname NAME              Container hostname (default: ha-ch-ingestor)
  --arch amd64|arm64           Default: detected from the Proxmox node
  --ip dhcp|CIDR               Default: dhcp (static example: 192.168.1.50/24)
  --gateway IP                 Gateway for a static IP
  --nameserver IP              DNS server for a static IP
  --bridge NAME                Default: first vmbr* on the node
  --storage NAME               Rootfs storage (default: first storage with rootdir)
  --template-storage NAME      Storage for LXC templates (default: first with vztmpl)
  --template NAME              Debian template (default: newest Debian standard)
  --cores N --memory MB --swap MB --disk GB
                               Defaults: 1 / 512 / 512 / 4

Build:
  --binary PATH                Use an existing linux binary instead of building
  --image TAG                  Local Docker tag (default: ha-ch-ingestor:deploy)

Other:
  --dry-run                    Validate and print the plan; touch nothing
  -h, --help                   This help

Examples:
  scripts/deploy-proxmox.sh --pve root@192.168.1.5 \
    --ha-url http://192.168.1.10:8123 \
    --clickhouse-url http://192.168.1.20:8123

  scripts/deploy-proxmox.sh --pve root@192.168.1.5 \
    --ha-url http://192.168.1.10:8123 \
    --clickhouse-url http://192.168.1.20:8123 \
    --clickhouse-password-file ~/.clickhouse-password \
    --ip 192.168.1.60/24 --gateway 192.168.1.1 --nameserver 192.168.1.1

Re-running the script updates the container it finds by hostname. It does not
change the network or resource settings of an existing container.

The service also receives these environment variables when set:
RETENTION_DAYS, BUFFER_CAPACITY, BUFFER_MAX_BYTES, INCLUDE_ATTRIBUTES,
EXCLUDE_ENTITIES, ATTRIBUTES_MAX_BYTES, LOG_FORMAT.
Retention only applies on table creation; updates never delete existing history.
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
parse_args() {
while [ $# -gt 0 ]; do
    case "$1" in
        --pve) [ $# -ge 2 ] || die "--pve needs a value"; PVE=$2; shift 2 ;;
        --ha-url) [ $# -ge 2 ] || die "--ha-url needs a value"; HA_URL=$2; shift 2 ;;
        --ha-ws-url) [ $# -ge 2 ] || die "--ha-ws-url needs a value"; HA_WS_URL=$2; shift 2 ;;
        --ha-token-file|--token-file)
            [ $# -ge 2 ] || die "--ha-token-file needs a value"; TOKEN_FILE=$2; shift 2 ;;
        --clickhouse-url)
            [ $# -ge 2 ] || die "--clickhouse-url needs a value"; CH_URL=$2; shift 2 ;;
        --clickhouse-user)
            [ $# -ge 2 ] || die "--clickhouse-user needs a value"; CH_USER=$2; shift 2 ;;
        --clickhouse-password)
            [ $# -ge 2 ] || die "--clickhouse-password needs a value"; CH_PASSWORD=$2; shift 2 ;;
        --clickhouse-password-file)
            [ $# -ge 2 ] || die "--clickhouse-password-file needs a value"; CH_PASSWORD_FILE=$2; shift 2 ;;
        --clickhouse-database)
            [ $# -ge 2 ] || die "--clickhouse-database needs a value"; CH_DB=$2; shift 2 ;;
        --clickhouse-table)
            [ $# -ge 2 ] || die "--clickhouse-table needs a value"; CH_TABLE=$2; shift 2 ;;
        --batch-size) [ $# -ge 2 ] || die "--batch-size needs a value"; BATCH_SIZE=$2; shift 2 ;;
        --flush-interval) [ $# -ge 2 ] || die "--flush-interval needs a value"; FLUSH_INTERVAL=$2; shift 2 ;;
        --log) [ $# -ge 2 ] || die "--log needs a value"; RUST_LOG_VALUE=$2; shift 2 ;;
        --no-create-schema) CREATE_SCHEMA=false; shift ;;
        --ctid) [ $# -ge 2 ] || die "--ctid needs a value"; CTID=$2; shift 2 ;;
        --hostname) [ $# -ge 2 ] || die "--hostname needs a value"; CT_HOSTNAME=$2; shift 2 ;;
        --storage) [ $# -ge 2 ] || die "--storage needs a value"; STORAGE=$2; shift 2 ;;
        --template-storage)
            [ $# -ge 2 ] || die "--template-storage needs a value"; TPL_STORAGE=$2; shift 2 ;;
        --template) [ $# -ge 2 ] || die "--template needs a value"; TEMPLATE=$2; shift 2 ;;
        --bridge) [ $# -ge 2 ] || die "--bridge needs a value"; BRIDGE=$2; shift 2 ;;
        --ip) [ $# -ge 2 ] || die "--ip needs a value"; IP=$2; shift 2 ;;
        --gateway) [ $# -ge 2 ] || die "--gateway needs a value"; GATEWAY=$2; shift 2 ;;
        --nameserver) [ $# -ge 2 ] || die "--nameserver needs a value"; NAMESERVER=$2; shift 2 ;;
        --cores) [ $# -ge 2 ] || die "--cores needs a value"; CORES=$2; shift 2 ;;
        --memory) [ $# -ge 2 ] || die "--memory needs a value"; MEMORY=$2; shift 2 ;;
        --swap) [ $# -ge 2 ] || die "--swap needs a value"; SWAP=$2; shift 2 ;;
        --disk) [ $# -ge 2 ] || die "--disk needs a value"; DISK=$2; shift 2 ;;
        --arch) [ $# -ge 2 ] || die "--arch needs a value"; ARCH=$2; shift 2 ;;
        --binary) [ $# -ge 2 ] || die "--binary needs a value"; BINARY=$2; shift 2 ;;
        --image) [ $# -ge 2 ] || die "--image needs a value"; IMAGE=$2; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown argument: $1 (try --help)" ;;
    esac
done

# ---------------------------------------------------------------------------
# Validation (before touching anything)
# ---------------------------------------------------------------------------
[ -n "$PVE" ] || die "--pve root@proxmox is required (try --help)"
[ -n "$HA_URL$HA_WS_URL" ] || die "--ha-url or --ha-ws-url is required"
[ -n "$CH_URL" ] || die "--clickhouse-url is required"

case "$ARCH" in ""|amd64|arm64) ;; *) die "--arch must be amd64 or arm64" ;; esac
[[ "$CT_HOSTNAME" =~ ^[a-z0-9][a-z0-9-]{0,62}$ ]] \
    || die "--hostname must be a lowercase DNS label"
[ -z "$CTID" ] || [[ "$CTID" =~ ^[0-9]+$ ]] || die "--ctid must be numeric"
for pair in "CORES:$CORES" "MEMORY:$MEMORY" "SWAP:$SWAP" "DISK:$DISK" \
            "BATCH_SIZE:$BATCH_SIZE" "FLUSH_INTERVAL:$FLUSH_INTERVAL"; do
    name=${pair%%:*}; value=${pair#*:}
    [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be numeric"
done
[ "$BATCH_SIZE" -ge 1 ] || die "BATCH_SIZE must be at least 1"
[ "$FLUSH_INTERVAL" -ge 1 ] || die "FLUSH_INTERVAL must be at least 1"
[[ "$CH_DB" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die "invalid ClickHouse database name"
[[ "$CH_TABLE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die "invalid ClickHouse table name"

if [ -n "$HA_WS_URL" ]; then
    case "$HA_WS_URL" in ws://*|wss://*) ;; *) die "--ha-ws-url must start with ws:// or wss://" ;; esac
else
    case "$HA_URL" in http://*|https://*) ;; *) die "--ha-url must start with http:// or https://" ;; esac
fi
case "$CH_URL" in http://*|https://*) ;; *) die "--clickhouse-url must start with http:// or https://" ;; esac

case "$IP" in
    dhcp) [ -z "$GATEWAY" ] || die "--gateway cannot be used with --ip dhcp" ;;
    */*) [[ "$IP" =~ ^[0-9a-fA-F:./]+$ ]] || die "--ip must be dhcp or CIDR (e.g. 192.168.1.50/24)" ;;
    *) die "--ip must be dhcp or CIDR (e.g. 192.168.1.50/24)" ;;
esac
[ -z "$GATEWAY" ] || [[ "$GATEWAY" =~ ^[0-9a-fA-F:.]+$ ]] || die "invalid --gateway"
[ -z "$NAMESERVER" ] || [[ "$NAMESERVER" =~ ^[0-9a-fA-F:.,\ ]+$ ]] || die "invalid --nameserver"

for value in "$STORAGE" "$TPL_STORAGE"; do
    [ -z "$value" ] || [[ "$value" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid storage name: $value"
done
[ -z "$TEMPLATE" ] || [[ "$TEMPLATE" =~ ^[A-Za-z0-9_.-]+\.tar\.(zst|gz)$ ]] \
    || die "invalid template name: $TEMPLATE"
[ -z "$BRIDGE" ] || [[ "$BRIDGE" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid bridge name: $BRIDGE"

for name in RETENTION_DAYS BUFFER_CAPACITY BUFFER_MAX_BYTES ATTRIBUTES_MAX_BYTES; do
    if [ "${!name+x}" = x ]; then
        [[ "${!name}" =~ ^[0-9]+$ ]] || die "$name must be numeric"
    fi
done
[ -z "${BUFFER_MAX_BYTES+x}" ] || [ "$BUFFER_MAX_BYTES" -ge 1 ] \
    || die "BUFFER_MAX_BYTES must be positive"
for name in CREATE_SCHEMA INCLUDE_ATTRIBUTES; do
    case "${!name-true}" in
        true|false|1|0|yes|no|on|off) ;;
        *) die "$name must be a boolean" ;;
    esac
done
case "${LOG_FORMAT:-text}" in text|json) ;; *) die "LOG_FORMAT must be text or json" ;; esac
case "${EXCLUDE_ENTITIES:-}" in *$'\n'*) die "EXCLUDE_ENTITIES must not contain newlines" ;; esac

# ClickHouse password: file > flag/environment > interactive prompt.
if [ -n "$CH_PASSWORD_FILE" ]; then
    [ -r "$CH_PASSWORD_FILE" ] || die "cannot read --clickhouse-password-file $CH_PASSWORD_FILE"
    CH_PASSWORD=$(cat "$CH_PASSWORD_FILE")
elif [ -n "$CH_PASSWORD" ]; then
    :
elif [ "$DRY_RUN" -eq 0 ] && [ -t 0 ]; then
    read -rsp "ClickHouse password (empty for none): " CH_PASSWORD
    printf '\n'
fi
case "$CH_PASSWORD" in *$'\n'*) die "the ClickHouse password must not contain newlines" ;; esac

# Home Assistant token: file first, then HA_TOKEN.
if [ -s "$TOKEN_FILE" ]; then
    :
elif [ -n "${HA_TOKEN:-}" ]; then
    :
else
    die "no Home Assistant token: put it in $TOKEN_FILE or set HA_TOKEN"
fi

for value in "$HA_URL" "$HA_WS_URL" "$CH_URL" "$CH_USER" "$CH_PASSWORD" "$CH_DB" "$CH_TABLE" "$RUST_LOG_VALUE"; do
    case "$value" in *$'\n'*) die "configuration values must not contain newlines" ;; esac
done

if [ "$DRY_RUN" -eq 1 ]; then
    if [ -s "$TOKEN_FILE" ]; then
        token_source=$TOKEN_FILE
    else
        token_source="HA_TOKEN environment"
    fi
    cat <<EOF
=== Dry run (nothing will be changed)

Proxmox node:  $PVE
Container:     ${CTID:-auto} ($CT_HOSTNAME, ${CORES} vCPU, ${MEMORY} MB RAM, ${DISK} GB disk)
Network:       ${IP}${GATEWAY:+, gateway $GATEWAY}${NAMESERVER:+, DNS $NAMESERVER}
Architecture:  ${ARCH:-auto-detect on the node}
Home Assistant: ${HA_WS_URL:-$HA_URL}
ClickHouse:    ${CH_URL%/}/ (${CH_DB}.${CH_TABLE} as ${CH_USER}, password $([ -n "$CH_PASSWORD" ] && echo set || echo empty))
Token:         ${token_source} (contents not printed)
Binary:        ${BINARY:-build with Docker for linux/${ARCH:-<arch>}}
Mode:          create a new LXC, or update the existing one with this hostname

EOF
    exit 0
fi
}

# ---------------------------------------------------------------------------
# SSH connection (one control master, so password auth prompts once)
# ---------------------------------------------------------------------------
SSH_CTL="/tmp/ha-ch-ingestor-ssh-$$"
SSH_OPTS=(-o ConnectTimeout=10 -o ControlMaster=auto -o ControlPath="$SSH_CTL" -o ControlPersist=10m)
LOCAL_TMP=""
REMOTE_TMP=""
SUDO=""

cleanup() {
    if [ -n "$REMOTE_TMP" ]; then
        case "$REMOTE_TMP" in
            /tmp/ha-ch-ingestor.*)
                ssh "${SSH_OPTS[@]}" -o BatchMode=yes "$PVE" "rm -rf '$REMOTE_TMP'" \
                    >/dev/null 2>&1 || true
                ;;
        esac
    fi
    if [ -n "$LOCAL_TMP" ]; then
        rm -rf "$LOCAL_TMP"
    fi
    if [ -n "$PVE" ]; then
        ssh "${SSH_OPTS[@]}" -o BatchMode=yes -O exit "$PVE" >/dev/null 2>&1 || true
        rm -f "$SSH_CTL"
    fi
}

ssh_run() { ssh "${SSH_OPTS[@]}" "$PVE" "$1"; }
run() {
    if [ -n "$SUDO" ]; then
        ssh_run "$SUDO $1"
    else
        ssh_run "$1"
    fi
}
scp_run() { scp "${SSH_OPTS[@]}" "$@"; }

setup_ssh() {
    step "Checking SSH and Proxmox on $PVE"
    if ! ssh_run 'true'; then
        die "cannot SSH to $PVE (try: ssh $PVE true)"
    fi
    if ! ssh_run 'command -v pct >/dev/null && command -v pveam >/dev/null && command -v pvesh >/dev/null && command -v pvesm >/dev/null'; then
        die "$PVE does not look like a Proxmox VE node"
    fi
    if [ "$(ssh_run 'id -u')" = "0" ]; then
        SUDO=""
    elif ssh_run 'sudo -n true' >/dev/null 2>&1; then
        SUDO="sudo"
    else
        die "log in as root or configure passwordless sudo for the SSH user"
    fi
}

detect_arch() {
    if [ -n "$ARCH" ]; then return; fi
    local machine
    machine=$(ssh_run 'uname -m')
    case "$machine" in
        x86_64) ARCH=amd64 ;;
        aarch64|arm64) ARCH=arm64 ;;
        *) die "unsupported node architecture: $machine (use --arch)" ;;
    esac
}

detect_bridge() {
    if [ -n "$BRIDGE" ]; then return; fi
    BRIDGE=$(ssh_run "ip -o link show 2>/dev/null | awk -F': ' '\$2 ~ /^vmbr/ {print \$2; exit}'" || true)
    [ -n "$BRIDGE" ] || BRIDGE=vmbr0
}

detect_rootfs_storage() {
    if [ -n "$STORAGE" ]; then return; fi
    local names pick
    names=$(run "pvesm status --content rootdir 2>/dev/null | awk 'NR>1 && \$3 == \"active\" {print \$1}'" || true)
    [ -n "$names" ] || die "no storage with 'rootdir' content; pass --storage"
    for pick in local-lvm local-zfs local; do
        if printf '%s\n' "$names" | grep -qx "$pick"; then
            STORAGE=$pick
            return
        fi
    done
    STORAGE=$(printf '%s\n' "$names" | head -n1)
}

detect_template_storage() {
    if [ -n "$TPL_STORAGE" ]; then return; fi
    local names
    names=$(run "pvesm status --content vztmpl 2>/dev/null | awk 'NR>1 && \$3 == \"active\" {print \$1}'" || true)
    [ -n "$names" ] || die "no storage with 'vztmpl' content; pass --template-storage"
    if printf '%s\n' "$names" | grep -qx local; then
        TPL_STORAGE=local
    else
        TPL_STORAGE=$(printf '%s\n' "$names" | head -n1)
    fi
}

detect_template() {
    if [ -n "$TEMPLATE" ]; then return; fi
    step "Looking for the newest Debian LXC template for $ARCH"
    run 'pveam update' >/dev/null 2>&1 || true
    TEMPLATE=$(run "pveam available --section system | awk '{print \$NF}' | grep -E '^debian-[0-9]+-standard_.*_${ARCH}\\.tar\\.(zst|gz)\$' | sort -V | tail -n1" || true)
    [ -n "$TEMPLATE" ] || die "no Debian template for $ARCH; pass --template (see: pveam available --section system)"
}

ensure_template() {
    if run "pveam list '$TPL_STORAGE' | grep -F '$TEMPLATE' >/dev/null 2>&1"; then
        return
    fi
    step "Downloading $TEMPLATE to $TPL_STORAGE"
    run "pveam download '$TPL_STORAGE' '$TEMPLATE'"
}

find_ctid() {
    local ids id host
    ids=$(run "pct list | awk 'NR>1 {print \$1}'" || true)
    for id in $ids; do
        host=$(run "pct config $id | sed -n 's/^hostname: //p'" || true)
        if [ "$host" = "$CT_HOSTNAME" ]; then
            printf '%s\n' "$id"
            return 0
        fi
    done
    return 1
}

create_container() {
    local net nameserver_opt=""
    net="name=eth0,bridge=$BRIDGE,ip=$IP"
    if [ -n "$GATEWAY" ]; then net="$net,gw=$GATEWAY"; fi
    if [ -n "$NAMESERVER" ]; then nameserver_opt="--nameserver '$NAMESERVER'"; fi

    step "Creating container $CTID with $TEMPLATE"
    run "pct create $CTID '$TPL_STORAGE:vztmpl/$TEMPLATE' \
        --hostname '$CT_HOSTNAME' \
        --cores $CORES --memory $MEMORY --swap $SWAP \
        --rootfs '$STORAGE:$DISK' \
        --net0 '$net' \
        $nameserver_opt \
        --unprivileged 1 --onboot 1 --ostype debian \
        --description 'Home Assistant to ClickHouse ingester'"
}

wait_for_container() {
    local i=0 state
    while [ $i -lt 30 ]; do
        if run "pct status $CTID" 2>/dev/null | grep -q running; then
            break
        fi
        sleep 1
        i=$((i + 1))
    done
    run "pct status $CTID" 2>/dev/null | grep -q running \
        || die "container $CTID did not start"

    i=0
    while [ $i -lt 30 ]; do
        state=$(run "pct exec $CTID -- systemctl is-system-running 2>/dev/null" || true)
        case "$state" in running|degraded) return ;; esac
        sleep 1
        i=$((i + 1))
    done
    die "systemd in container $CTID did not become ready"
}

build_binary() {
    if [ -n "$BINARY" ]; then
        [ -x "$BINARY" ] || die "--binary $BINARY is not executable"
        cp "$BINARY" "$LOCAL_TMP/ingestor"
        return
    fi
    command -v docker >/dev/null 2>&1 || die "docker is required to build the binary (or pass --binary)"
    docker info >/dev/null 2>&1 || die "docker is not running"
    step "Building the linux/$ARCH binary with Docker"
    docker build --platform "linux/$ARCH" -t "$IMAGE" "$REPO_DIR"
    local cid
    cid=$(docker create --platform "linux/$ARCH" "$IMAGE")
    docker cp "$cid:/ingestor" "$LOCAL_TMP/ingestor"
    docker rm "$cid" >/dev/null
}

verify_binary() {
    chmod 0755 "$LOCAL_TMP/ingestor"
    command -v file >/dev/null 2>&1 || return
    case "$ARCH" in
        amd64) file "$LOCAL_TMP/ingestor" | grep -q 'x86-64' || die "the binary is not amd64" ;;
        arm64) file "$LOCAL_TMP/ingestor" | grep -q 'ARM aarch64' || die "the binary is not arm64" ;;
    esac
}

sd_quote() {
    local value=$1
    case "$value" in *$'\n'*) die "configuration values must not contain newlines" ;; esac
    value=${value//\\/\\\\}
    value=${value//\"/\\\"}
    printf '"%s"' "$value"
}

write_files() {
    umask 077
    if [ -s "$TOKEN_FILE" ]; then
        cp "$TOKEN_FILE" "$LOCAL_TMP/ha-ingestor.token"
    else
        printf '%s' "$HA_TOKEN" > "$LOCAL_TMP/ha-ingestor.token"
    fi
    chmod 0600 "$LOCAL_TMP/ha-ingestor.token"
    [ -s "$LOCAL_TMP/ha-ingestor.token" ] || die "the Home Assistant token is empty"

    {
        if [ -n "$HA_WS_URL" ]; then
            printf 'HA_WS_URL=%s\n' "$(sd_quote "$HA_WS_URL")"
        else
            printf 'HA_URL=%s\n' "$(sd_quote "$HA_URL")"
        fi
        printf 'HA_TOKEN_FILE=/etc/ha-ingestor.token\n'
        printf 'CLICKHOUSE_URL=%s\n' "$(sd_quote "$CH_URL")"
        printf 'CLICKHOUSE_USER=%s\n' "$(sd_quote "$CH_USER")"
        printf 'CLICKHOUSE_PASSWORD=%s\n' "$(sd_quote "$CH_PASSWORD")"
        printf 'CLICKHOUSE_DATABASE=%s\n' "$(sd_quote "$CH_DB")"
        printf 'CLICKHOUSE_TABLE=%s\n' "$(sd_quote "$CH_TABLE")"
        printf 'CREATE_SCHEMA=%s\n' "$CREATE_SCHEMA"
        printf 'BATCH_SIZE=%s\n' "$BATCH_SIZE"
        printf 'FLUSH_INTERVAL=%s\n' "$FLUSH_INTERVAL"
        printf 'RUST_LOG=%s\n' "$(sd_quote "$RUST_LOG_VALUE")"
        for name in RETENTION_DAYS BUFFER_CAPACITY BUFFER_MAX_BYTES INCLUDE_ATTRIBUTES EXCLUDE_ENTITIES ATTRIBUTES_MAX_BYTES LOG_FORMAT; do
            if [ "${!name+x}" = x ]; then
                printf '%s=%s\n' "$name" "$(sd_quote "${!name}")"
            fi
        done
    } > "$LOCAL_TMP/ha-ingestor.env"
    chmod 0600 "$LOCAL_TMP/ha-ingestor.env"

    cat > "$LOCAL_TMP/ha-ingestor.service" <<'UNIT'
[Unit]
Description=Home Assistant to ClickHouse ingester
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ha-ingestor
Group=ha-ingestor
EnvironmentFile=/etc/ha-ingestor.env
ExecStart=/usr/local/bin/ha-ch-ingestor
Restart=always
RestartSec=5
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
UNIT
    chmod 0644 "$LOCAL_TMP/ha-ingestor.service"
}

install_ca() {
    if run "pct exec $CTID -- test -f /etc/ssl/certs/ca-certificates.crt" 2>/dev/null; then
        return
    fi
    step "Installing ca-certificates in the container"
    if ! run "pct exec $CTID -- sh -c 'timeout 120 apt-get update -qq && DEBIAN_FRONTEND=noninteractive timeout 120 apt-get install -y -qq ca-certificates'"; then
        warn "could not install ca-certificates; only plain ws:// and http:// will work"
    fi
}

install_files() {
    step "Installing the binary, config and systemd unit"
    run "pct exec $CTID -- systemctl stop ha-ch-ingestor.service >/dev/null 2>&1 || true"

    REMOTE_TMP=$(ssh_run 'umask 077; mktemp -d /tmp/ha-ch-ingestor.XXXXXX')
    case "$REMOTE_TMP" in
        /tmp/ha-ch-ingestor.*) ;;
        *) die "unexpected remote temp path: $REMOTE_TMP" ;;
    esac

    scp_run -p \
        "$LOCAL_TMP/ingestor" \
        "$LOCAL_TMP/ha-ingestor.token" \
        "$LOCAL_TMP/ha-ingestor.env" \
        "$LOCAL_TMP/ha-ingestor.service" \
        "$PVE:$REMOTE_TMP/"

    run "pct push $CTID '$REMOTE_TMP/ingestor' /usr/local/bin/ha-ch-ingestor --perms 0755"
    run "pct push $CTID '$REMOTE_TMP/ha-ingestor.token' /etc/ha-ingestor.token --perms 0600"
    run "pct push $CTID '$REMOTE_TMP/ha-ingestor.env' /etc/ha-ingestor.env --perms 0640"
    run "pct push $CTID '$REMOTE_TMP/ha-ingestor.service' /etc/systemd/system/ha-ch-ingestor.service --perms 0644"
    run "rm -rf '$REMOTE_TMP'"
    REMOTE_TMP=""

    run "pct exec $CTID -- sh -c 'getent group ha-ingestor >/dev/null || groupadd --system ha-ingestor'"
    run "pct exec $CTID -- sh -c 'getent passwd ha-ingestor >/dev/null || useradd --system --gid ha-ingestor --home /nonexistent --shell /usr/sbin/nologin ha-ingestor'"
    run "pct exec $CTID -- sh -c 'chown ha-ingestor:ha-ingestor /etc/ha-ingestor.token && chmod 0600 /etc/ha-ingestor.token'"
    run "pct exec $CTID -- sh -c 'chown root:ha-ingestor /etc/ha-ingestor.env && chmod 0640 /etc/ha-ingestor.env'"

    run "pct exec $CTID -- systemctl daemon-reload"
    run "pct exec $CTID -- systemctl enable ha-ch-ingestor.service"
    run "pct exec $CTID -- systemctl restart ha-ch-ingestor.service"
}

show_status() {
    sleep 3
    local logs ip
    logs=$(run "pct exec $CTID -- journalctl -u ha-ch-ingestor.service -n 40 --no-pager" || true)
    printf '%s\n' "$logs"

    if ! run "pct exec $CTID -- systemctl is-active --quiet ha-ch-ingestor.service"; then
        die "the service is not active; see the logs above"
    fi
    if printf '%s' "$logs" | grep -q 'rejected the token'; then
        warn "Home Assistant rejected the token; create a new long-lived token and re-run"
    fi
    if printf '%s' "$logs" | grep -q 'missing columns'; then
        warn "the ClickHouse table is missing time_fired; use a new --clickhouse-table (e.g. states_v2) or migrate it"
    fi

    local check_subscribed=1
    case "$RUST_LOG_VALUE" in
        ""|*info*|*debug*|*trace*) ;;
        *) check_subscribed=0 ;;
    esac
    if [ "$check_subscribed" -eq 1 ] \
        && ! printf '%s' "$logs" | grep -q 'subscribed to state_changed'; then
        warn "not subscribed yet; Home Assistant or ClickHouse may still be starting (the ingester retries)"
    fi

    ip=$(run "pct exec $CTID -- hostname -I" || true)
    printf '\n'
    printf 'Container:  %s (%s)\n' "$CTID" "$CT_HOSTNAME"
    if [ -n "$ip" ]; then printf 'Address:    %s\n' "$ip"; fi
    printf 'Logs:       ssh %s %spct exec %s -- journalctl -u ha-ch-ingestor -f\n' \
        "$PVE" "${SUDO:+$SUDO }" "$CTID"
    printf 'Update:     re-run this script (or pass --ctid %s)\n' "$CTID"
    printf 'Query:      curl -s --user ha_reader "%s/" --data-binary "SELECT count() FROM %s.%s"\n' \
        "${CH_URL%/}" "$CH_DB" "$CH_TABLE"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
parse_args "$@"
trap cleanup EXIT
setup_ssh
detect_arch
[ -n "$ARCH" ] || die "could not determine the node architecture"

# Build and stage everything before creating a container, so a failed build
# does not leave half a deployment behind.
LOCAL_TMP=$(mktemp -d "${TMPDIR:-/tmp}/ha-ch-ingestor-deploy.XXXXXX")
chmod 0700 "$LOCAL_TMP"
build_binary
verify_binary
write_files

CREATE=0
if [ -n "$CTID" ]; then
    if run "pct status $CTID" >/dev/null 2>&1; then
        step "Using existing container $CTID; updating it"
    else
        CREATE=1
    fi
elif CTID=$(find_ctid); then
    step "Found existing container $CTID; updating it"
else
    CTID=$(run 'pvesh get /cluster/nextid' | tail -n1 | tr -d '[:space:]"')
    [[ "$CTID" =~ ^[0-9]+$ ]] || die "could not determine the next container ID; pass --ctid"
    CREATE=1
fi

if [ "$CREATE" -eq 1 ]; then
    detect_rootfs_storage
    detect_template_storage
    detect_bridge
    detect_template
    ensure_template
    create_container
fi

# Apply the boot policy on updates too.
run "pct set $CTID --onboot 1"
if ! run "pct status $CTID" 2>/dev/null | grep -q running; then
    step "Starting container $CTID"
    run "pct start $CTID"
fi
wait_for_container

install_ca
install_files
show_status

}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
