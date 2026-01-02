#!/bin/bash
# bench_network.sh - Benchmark adaptive compression at different network speeds
set -e
cd "$(dirname "$0")/.."

B='\033[34m'; C='\033[36m'; RST='\033[0m'
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

echo -e "${B}Adaptive Compression vs Network Speed${RST}"
echo

TD=$(mktemp -d)
trap "rm -rf $TD" EXIT

# Test configs: pv_rate, data_mb, display_name
# Longer tests to see more adaptation
TESTS=(
    "500k:20:500 KB/s"
    "2m:100:2 MB/s"
    "10m:400:10 MB/s"
)

printf "%-12s %6s %8s %8s %8s %s\n" "Bandwidth" "Data" "Time" "Sent" "Ratio" "Compression"
echo "──────────── ────── ──────── ──────── ──────── ────────────────────"

for spec in "${TESTS[@]}"; do
    IFS=':' read -r RATE DATA_MB NAME <<< "$spec"

    # Create test data
    SRC="$TD/src_$DATA_MB"
    DST="$TD/dst_$DATA_MB"
    rm -rf "$SRC" "$DST"; mkdir -p "$SRC" "$DST"

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

    # FIFOs for bidirectional communication
    S2R="$TD/s2r_$$"; R2S="$TD/r2s_$$"
    rm -f "$S2R" "$R2S"
    mkfifo "$S2R" "$R2S"

    # Receiver: reads from s2r, writes to r2s
    uv run "$PSYNC" --server "$DST" < "$S2R" > "$R2S" 2>/dev/null &
    RPID=$!

    # Sender with rate limit on output: reads from r2s, output through pv to s2r
    # Capture stderr separately for stats
    START=$(date +%s.%N)
    uv run "$PSYNC" -vz --stats "$SRC/" --pipe-out < "$R2S" 2>"$TD/sender.log" | pv -q -L "$RATE" > "$S2R"
    END=$(date +%s.%N)
    OUT=$(cat "$TD/sender.log")
    wait $RPID 2>/dev/null || true
    rm -f "$S2R" "$R2S"

    WALL=$(echo "$END - $START" | bc)
    SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
    SENT=${SENT:-$SRC_SIZE}
    SENT_MB=$((SENT / 1024 / 1024))
    RATIO=$(echo "scale=1; $SENT * 100 / $SRC_SIZE" | bc)

    # Extract compression level changes
    LEVELS=$(echo "$OUT" | grep -oP '\[zstd\] level \d+→\d+' | tr '\n' ' ')
    [ -z "$LEVELS" ] && LEVELS="level 3 (no change)"

    printf "%-12s %4dMB %6.1fs %6dMB %6.1f%% %s\n" \
        "$NAME" "$SRC_MB" "$WALL" "$SENT_MB" "$RATIO" "$LEVELS"
done

echo
echo "Expected: Slower network → higher compression (more CPU headroom)"
