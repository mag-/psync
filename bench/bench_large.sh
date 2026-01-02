#!/bin/bash
# bench_large.sh - Large-scale benchmark (100GB) with adaptive compression
set -e
cd "$(dirname "$0")"

G='\033[32m'; Y='\033[33m'; B='\033[34m'; R='\033[31m'; C='\033[36m'; RST='\033[0m'

SIZE_GB=${1:-10}  # Default 10GB, pass argument for more
CORPUS_BASE="bench/silesia"
PSYNC="./psync.py"

echo -e "${B}╔════════════════════════════════════════════════════════════════════╗${RST}"
echo -e "${B}║           psync Large-Scale Benchmark (${SIZE_GB}GB)                       ║${RST}"
echo -e "${B}╚════════════════════════════════════════════════════════════════════╝${RST}"
echo

# Ensure base corpus exists
if [ ! -d "$CORPUS_BASE" ]; then
    echo -e "${Y}Downloading Silesia compression corpus...${RST}"
    mkdir -p bench
    curl -L -o bench/silesia.zip "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip"
    unzip -o bench/silesia.zip -d bench/silesia
    rm bench/silesia.zip
fi

CORPUS_SIZE=$(du -sb $CORPUS_BASE | cut -f1)
CORPUS_MB=$((CORPUS_SIZE / 1024 / 1024))
TARGET_SIZE=$((SIZE_GB * 1024 * 1024 * 1024))
COPIES=$((TARGET_SIZE / CORPUS_SIZE + 1))

echo -e "${B}Base corpus: ${CORPUS_MB} MB${RST}"
echo -e "${B}Creating ${SIZE_GB}GB test data (~${COPIES} copies)...${RST}"

# Create large corpus by replicating with variations
LARGE_CORPUS="/tmp/psync_bench_${SIZE_GB}gb"
rm -rf "$LARGE_CORPUS"
mkdir -p "$LARGE_CORPUS"

CURRENT_SIZE=0
COPY_NUM=0
while [ $CURRENT_SIZE -lt $TARGET_SIZE ]; do
    for f in "$CORPUS_BASE"/*; do
        [ -f "$f" ] || continue
        BASENAME=$(basename "$f")
        DEST="$LARGE_CORPUS/${COPY_NUM}_${BASENAME}"
        cp "$f" "$DEST"
        # Add small variation to each copy (append copy number)
        echo "Copy $COPY_NUM of $BASENAME" >> "$DEST"
        CURRENT_SIZE=$((CURRENT_SIZE + $(stat -c%s "$f")))
        if [ $CURRENT_SIZE -ge $TARGET_SIZE ]; then
            break
        fi
    done
    COPY_NUM=$((COPY_NUM + 1))
    printf "\r  Progress: %.1f GB" "$(echo "scale=1; $CURRENT_SIZE / 1024 / 1024 / 1024" | bc)"
done
echo

ACTUAL_SIZE=$(du -sb "$LARGE_CORPUS" | cut -f1)
ACTUAL_GB=$(echo "scale=2; $ACTUAL_SIZE / 1024 / 1024 / 1024" | bc)
FILE_COUNT=$(ls "$LARGE_CORPUS" | wc -l)
echo -e "${G}Created: ${ACTUAL_GB} GB in ${FILE_COUNT} files${RST}"
echo

# Setup destination
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR $LARGE_CORPUS" EXIT

header() {
    printf "  %-6s %-18s %10s %10s %12s %8s\n" "Tool" "Scenario" "Time" "Sent" "Throughput" "Ratio"
    echo "  ────── ────────────────── ────────── ────────── ──────────── ────────"
}

fmt_result() {
    local name=$1 scenario=$2 wall=$3 sent=$4 total=$5
    local throughput=$(echo "scale=1; $total / 1024 / 1024 / 1024 / $wall" | bc 2>/dev/null || echo "0")
    local sent_gb=$(echo "scale=2; $sent / 1024 / 1024 / 1024" | bc 2>/dev/null || echo "0")
    local ratio=$(echo "scale=1; $sent * 100 / $total" | bc 2>/dev/null || echo "0")
    printf "  ${C}%-6s${RST} %-18s %8.1fs %8.2f GB %9.2f GB/s %6.1f%%\n" \
        "$name" "$scenario" "$wall" "$sent_gb" "$throughput" "$ratio"
}

# ═══════════════════════════════════════════════════════════════════════════════
echo -e "${Y}━━━ Test 1: Full Sync with Adaptive Compression ━━━${RST}"
header

# psync with adaptive compression
DST="$TMPDIR/psync_adaptive"
mkdir -p "$DST"
echo -e "  ${B}Running psync with adaptive zstd...${RST}"
START=$(date +%s.%N)
OUT=$(uv run "$PSYNC" -avz --stats "$LARGE_CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
echo "$OUT" | grep -E "\[adaptive\]|Total bytes" || true
fmt_result "psync" "adaptive -z" "$WALL" "${SENT:-$ACTUAL_SIZE}" "$ACTUAL_SIZE"

# rsync for comparison
DST="$TMPDIR/rsync_z"
mkdir -p "$DST"
echo -e "\n  ${B}Running rsync -z for comparison...${RST}"
START=$(date +%s.%N)
OUT=$(rsync -avz --stats -e "ssh -o StrictHostKeyChecking=no" "$LARGE_CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "rsync" "full -z" "$WALL" "${SENT:-$ACTUAL_SIZE}" "$ACTUAL_SIZE"

# ═══════════════════════════════════════════════════════════════════════════════
echo
echo -e "${Y}━━━ Test 2: Incremental (No Changes) ━━━${RST}"
header

# psync incremental
DST="$TMPDIR/psync_adaptive"
START=$(date +%s.%N)
OUT=$(uv run "$PSYNC" -avz --stats "$LARGE_CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "psync" "no_changes" "$WALL" "${SENT:-0}" "$ACTUAL_SIZE"

# rsync incremental
DST="$TMPDIR/rsync_z"
START=$(date +%s.%N)
OUT=$(rsync -avz --stats -e "ssh -o StrictHostKeyChecking=no" "$LARGE_CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "rsync" "no_changes" "$WALL" "${SENT:-0}" "$ACTUAL_SIZE"

# ═══════════════════════════════════════════════════════════════════════════════
echo
echo -e "${B}════════════════════════════════════════════════════════════════════${RST}"
echo -e "${B}Notes:${RST}"
echo -e "  • Adaptive compression adjusts zstd level 1-19 based on CPU headroom"
echo -e "  • Higher level = better ratio but slower"
echo -e "  • Watch for [adaptive] messages showing level changes"
echo -e "${B}════════════════════════════════════════════════════════════════════${RST}"
