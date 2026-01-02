#!/bin/bash
# bench.sh - Benchmark psync vs rsync on Silesia corpus
set -e
cd "$(dirname "$0")"

G='\033[32m'; Y='\033[33m'; B='\033[34m'; R='\033[31m'; C='\033[36m'; RST='\033[0m'

echo -e "${B}╔════════════════════════════════════════════════════════════════════╗${RST}"
echo -e "${B}║                    psync vs rsync Benchmark                        ║${RST}"
echo -e "${B}╚════════════════════════════════════════════════════════════════════╝${RST}"
echo

# Download Silesia corpus if needed
if [ ! -d "bench/silesia" ]; then
    echo -e "${Y}Downloading Silesia compression corpus...${RST}"
    mkdir -p bench
    curl -L -o bench/silesia.zip "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip"
    unzip -o bench/silesia.zip -d bench/silesia
    rm bench/silesia.zip
fi

CORPUS="bench/silesia"
PSYNC="./psync.py"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

CORPUS_SIZE=$(du -sb $CORPUS | cut -f1)
CORPUS_MB=$(echo "scale=1; $CORPUS_SIZE / 1024 / 1024" | bc)
echo -e "${B}Corpus: ${CORPUS_MB} MB in $(ls $CORPUS | wc -l) files${RST}"
echo

fmt_result() {
    local name=$1 scenario=$2 wall=$3 sent=$4 total=$5
    local throughput=$(echo "scale=1; $total / 1024 / 1024 / $wall" | bc 2>/dev/null || echo "0")
    local sent_mb=$(echo "scale=2; $sent / 1024 / 1024" | bc 2>/dev/null || echo "0")
    local pct=$(echo "scale=1; $sent * 100 / $total" | bc 2>/dev/null || echo "0")
    printf "  ${C}%-6s${RST} %-18s %6.2fs  %7.1f MB/s  ${G}%7.2f MB${RST} (%5.1f%%)\n" \
        "$name" "$scenario" "$wall" "$throughput" "$sent_mb" "$pct"
}

header() {
    printf "  %-6s %-18s %8s  %12s  %12s\n" "Tool" "Scenario" "Time" "Throughput" "Bytes Sent"
    echo "  ────── ────────────────── ──────── ──────────── ────────────────────"
}

# ═══════════════════════════════════════════════════════════════════════════════
echo -e "${Y}━━━ Scenario 1: Full Initial Sync (SSH to localhost) ━━━${RST}"
header

# rsync
DST="$TMPDIR/rsync_full"
rm -rf "$DST"; mkdir -p "$DST"
START=$(date +%s.%N)
OUT=$(rsync -a --stats -e "ssh -o StrictHostKeyChecking=no" "$CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "rsync" "full_sync" "$WALL" "${SENT:-$CORPUS_SIZE}" "$CORPUS_SIZE"

# psync
DST="$TMPDIR/psync_full"
rm -rf "$DST"; mkdir -p "$DST"
START=$(date +%s.%N)
OUT=$(uv run "$PSYNC" -a --stats "$CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "psync" "full_sync" "$WALL" "${SENT:-$CORPUS_SIZE}" "$CORPUS_SIZE"

# ═══════════════════════════════════════════════════════════════════════════════
echo
echo -e "${Y}━━━ Scenario 2: No Changes (Incremental) ━━━${RST}"
header

# rsync
DST="$TMPDIR/rsync_full"
START=$(date +%s.%N)
OUT=$(rsync -a --stats -e "ssh -o StrictHostKeyChecking=no" "$CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "rsync" "no_changes" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# psync
DST="$TMPDIR/psync_full"
START=$(date +%s.%N)
OUT=$(uv run "$PSYNC" -a --stats "$CORPUS/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "psync" "no_changes" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# ═══════════════════════════════════════════════════════════════════════════════
echo
echo -e "${Y}━━━ Scenario 3: 1% Modification (Delta Test) ━━━${RST}"
header

# Create modified corpus
SRC_MOD="$TMPDIR/modified1"
cp -r "$CORPUS" "$SRC_MOD"
for f in "$SRC_MOD"/*; do
    [ -f "$f" ] || continue
    SIZE=$(stat -c%s "$f")
    MODIFY=$((SIZE / 100))
    dd if=/dev/urandom of="$f" bs=1 count=$MODIFY seek=$((SIZE / 2)) conv=notrunc 2>/dev/null
done

# rsync (has delta for SSH)
DST="$TMPDIR/rsync_full"
START=$(date +%s.%N)
OUT=$(rsync -a --stats -e "ssh -o StrictHostKeyChecking=no" "$SRC_MOD/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "rsync" "1%_modified" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# psync (has delta)
DST="$TMPDIR/psync_full"
START=$(date +%s.%N)
OUT=$(uv run "$PSYNC" -a --stats "$SRC_MOD/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "psync" "1%_modified" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# ═══════════════════════════════════════════════════════════════════════════════
echo
echo -e "${Y}━━━ Scenario 4: 10% Modification ━━━${RST}"
header

# Modify more
for f in "$SRC_MOD"/*; do
    [ -f "$f" ] || continue
    SIZE=$(stat -c%s "$f")
    MODIFY=$((SIZE / 10))
    dd if=/dev/urandom of="$f" bs=1 count=$MODIFY seek=$((SIZE / 3)) conv=notrunc 2>/dev/null
done

# rsync
DST="$TMPDIR/rsync_full"
START=$(date +%s.%N)
OUT=$(rsync -a --stats -e "ssh -o StrictHostKeyChecking=no" "$SRC_MOD/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "rsync" "10%_modified" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# psync
DST="$TMPDIR/psync_full"
START=$(date +%s.%N)
OUT=$(uv run "$PSYNC" -a --stats "$SRC_MOD/" "localhost:$DST/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "psync" "10%_modified" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# ═══════════════════════════════════════════════════════════════════════════════
echo
echo -e "${Y}━━━ Scenario 5: With Compression (-z) ━━━${RST}"
header

# Fresh sync with compression
DST_R="$TMPDIR/rsync_z"
DST_P="$TMPDIR/psync_z"
rm -rf "$DST_R" "$DST_P"; mkdir -p "$DST_R" "$DST_P"

# rsync -z
START=$(date +%s.%N)
OUT=$(rsync -az --stats -e "ssh -o StrictHostKeyChecking=no" "$CORPUS/" "localhost:$DST_R/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "rsync" "full_sync -z" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# psync -z
START=$(date +%s.%N)
OUT=$(uv run "$PSYNC" -az --stats "$CORPUS/" "localhost:$DST_P/" 2>&1)
END=$(date +%s.%N)
WALL=$(echo "$END - $START" | bc)
SENT=$(echo "$OUT" | grep "Total bytes sent" | awk -F: '{gsub(/,/,"",$2); print $2}')
fmt_result "psync" "full_sync -z" "$WALL" "${SENT:-0}" "$CORPUS_SIZE"

# ═══════════════════════════════════════════════════════════════════════════════
echo
echo -e "${B}════════════════════════════════════════════════════════════════════${RST}"
echo -e "${B}Notes:${RST}"
echo -e "  • All tests via SSH to localhost (forces delta algorithm)"
echo -e "  • rsync: C implementation, zlib compression"
echo -e "  • psync: Pure Python, xxhash + zstd compression"
echo -e "  • Corpus: Silesia (${CORPUS_MB} MB) - mixed text/binary/xml"
echo -e "${B}════════════════════════════════════════════════════════════════════${RST}"
