#!/bin/bash
# bench_network.sh - Compare psync adaptive compression vs rsync at different speeds
set -e
cd "$(dirname "$0")/.."

B='\033[34m'; G='\033[32m'; Y='\033[33m'; C='\033[36m'; RST='\033[0m'
CORPUS="bench/silesia"
PSYNC="./psync.py"

command -v pv >/dev/null || { echo "Install pv: apt install pv"; exit 1; }

[ -d "$CORPUS" ] || {
    echo "Downloading Silesia corpus..."
    mkdir -p bench
    curl -sL -o bench/silesia.zip "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip"
    unzip -qo bench/silesia.zip -d bench/silesia
    rm bench/silesia.zip
}

echo -e "${B}╔══════════════════════════════════════════════════════════════╗${RST}"
echo -e "${B}║     psync (adaptive zstd) vs rsync (zlib) by Bandwidth       ║${RST}"
echo -e "${B}╚══════════════════════════════════════════════════════════════╝${RST}"
echo

TD=$(mktemp -d)
trap "rm -rf $TD" EXIT

# Test configs: pv_rate, data_mb, display_name
TESTS=(
    "1m:50:1 MB/s"
    "5m:100:5 MB/s"
    "20m:200:20 MB/s"
)

for spec in "${TESTS[@]}"; do
    IFS=':' read -r RATE DATA_MB NAME <<< "$spec"

    # Create test data
    SRC="$TD/src_$DATA_MB"
    rm -rf "$SRC"; mkdir -p "$SRC"

    TARGET=$((DATA_MB * 1024 * 1024))
    SIZE=0; N=0
    while [ $SIZE -lt $TARGET ]; do
        for f in "$CORPUS"/*; do
            [ -f "$f" ] || continue
            cp "$f" "$SRC/${N}_$(basename "$f")"
            SIZE=$((SIZE + $(stat -c%s "$f")))
            [ $SIZE -ge $TARGET ] && break
        done
        N=$((N + 1))
    done
    SRC_SIZE=$(du -sb "$SRC" | cut -f1)
    SRC_MB=$((SRC_SIZE / 1024 / 1024))

    echo -e "${Y}━━━ ${NAME} (${SRC_MB}MB data) ━━━${RST}"
    printf "  %-8s %8s %10s %8s %s\n" "Tool" "Time" "Sent" "Ratio" "Notes"
    echo "  ──────── ──────── ────────── ──────── ─────────────────────"

    # === RSYNC over SSH (to enable compression) with bwlimit ===
    DST="$TD/rsync_dst"
    rm -rf "$DST"; mkdir -p "$DST"

    # rsync --bwlimit is in KiB/s: 1m=1MB/s=1024KiB/s
    BW_KIB=$(echo "${RATE%m} * 1024" | bc)
    START=$(date +%s.%N)
    # Use localhost SSH so -z compression actually works
    OUT=$(rsync -az --stats --bwlimit="$BW_KIB" -e "ssh -o StrictHostKeyChecking=no -o Compression=no" "$SRC/" "localhost:$DST/" 2>&1) || true
    END=$(date +%s.%N)

    WALL=$(echo "$END - $START" | bc)
    SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
    SENT=${SENT:-$SRC_SIZE}
    SENT_MB=$(echo "scale=1; $SENT / 1024 / 1024" | bc)
    RATIO=$(echo "scale=1; $SENT * 100 / $SRC_SIZE" | bc)
    printf "  ${C}%-8s${RST} %6.1fs %8.1fMB %6.1f%% %s\n" "rsync" "$WALL" "$SENT_MB" "$RATIO" "zlib=6, bwlimit=${RATE}"

    # === PSYNC ===
    DST="$TD/psync_dst"
    rm -rf "$DST"; mkdir -p "$DST"
    S2R="$TD/s2r_p"; R2S="$TD/r2s_p"
    rm -f "$S2R" "$R2S"; mkfifo "$S2R" "$R2S"

    uv run "$PSYNC" --server "$DST" < "$S2R" > "$R2S" 2>/dev/null &
    RPID=$!

    START=$(date +%s.%N)
    uv run "$PSYNC" -vz --stats "$SRC/" --pipe-out < "$R2S" 2>"$TD/psync.log" | pv -q -L "$RATE" > "$S2R"
    END=$(date +%s.%N)
    wait $RPID 2>/dev/null || true
    rm -f "$S2R" "$R2S"

    OUT=$(cat "$TD/psync.log")
    WALL=$(echo "$END - $START" | bc)
    SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
    SENT=${SENT:-$SRC_SIZE}
    SENT_MB=$(echo "scale=1; $SENT / 1024 / 1024" | bc)
    RATIO=$(echo "scale=1; $SENT * 100 / $SRC_SIZE" | bc)

    # Get final level
    FINAL_LEVEL=$(echo "$OUT" | grep -oP 'level \d+→\K\d+' | tail -1)
    FINAL_LEVEL=${FINAL_LEVEL:-3}
    CHANGES=$(echo "$OUT" | grep -c '\[zstd\]' || echo 0)

    printf "  ${G}%-8s${RST} %6.1fs %8.1fMB %6.1f%% %s\n" "psync" "$WALL" "$SENT_MB" "$RATIO" "zstd adaptive→$FINAL_LEVEL (${CHANGES} changes)"
    echo
done

echo -e "${B}════════════════════════════════════════════════════════════════${RST}"
echo -e "${B}Notes:${RST}"
echo "  • rsync: fixed zlib level 6, SSH localhost, --bwlimit rate limiting"
echo "  • psync: adaptive zstd (level 1-19), named pipes, pv rate limiting"
echo "  • Lower ratio = better compression"
echo -e "${B}════════════════════════════════════════════════════════════════${RST}"
