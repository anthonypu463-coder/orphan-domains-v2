#!/usr/bin/env bash
set -euo pipefail
OUT="data/interim/accessions.txt"
LOG="logs/02b_fetch_accessions_server_filtered.log"
SIZE=500
mkdir -p "$(dirname "$OUT")" "$(dirname "$LOG")"
: > "$OUT"; : > "$LOG"

offset=0
while :; do
  echo "[page] offset=$offset" | tee -a "$LOG"
  chunk=$(mktemp)
  curl -sS -G 'https://rest.uniprot.org/uniprotkb/search' \
    --data-urlencode 'query=reviewed:false AND fragment:false AND (protein_name:"uncharacterized protein" OR protein_name:"uncharacterised protein" OR protein_name:"hypothetical protein") AND (NOT database:Pfam) AND (NOT database:InterPro)' \
    --data-urlencode 'format=tsv' \
    --data-urlencode 'fields=accession' \
    --data-urlencode "size=$SIZE" \
    --data-urlencode "offset=$offset" > "$chunk"

  rows=$(( $(wc -l < "$chunk") - 1 ))   # minus header
  if [ "$rows" -le 0 ]; then rm -f "$chunk"; break; fi

  tail -n +2 "$chunk" >> "$OUT"
  rm -f "$chunk"

  echo "[page] added=$rows total=$(wc -l < "$OUT")" | tee -a "$LOG"

  if [ "$rows" -lt "$SIZE" ]; then break; fi
  offset=$(( offset + SIZE ))
  sleep 0.2
done

# Deduplicate and finalize
sort -u -o "$OUT" "$OUT"
echo "[done] unique accessions: $(wc -l < "$OUT") -> $OUT" | tee -a "$LOG"
