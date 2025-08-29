#!/usr/bin/env python3
import csv, re, os, sys

TSV = "data/interim/uniprot_candidates_noPF.tsv"
OUT = "data/interim/accessions.txt"
SUM = "data/interim/summary_accessions_anycolumn.txt"

# UniProt primary accession patterns:
# 6-char: [A-Z][0-9][A-Z0-9]{3}[0-9]
# 10-char: A0A[0-9A-Z]{7}
acc_re = re.compile(r'(?:^|[^A-Z0-9])((?:[A-Z][0-9][A-Z0-9]{3}[0-9]|A0A[0-9A-Z]{7})(?:-\d+)?)(?:[^A-Z0-9]|$)')

if not os.path.exists(TSV):
    print(f"ERROR: missing {TSV}", file=sys.stderr); sys.exit(2)

seen = set()
accs = []
rows = 0
hits = 0

with open(TSV, newline="") as f:
    r = csv.reader(f, delimiter="\t")
    header = next(r, [])
    for row in r:
        rows += 1
        # Scan every field; take the first accession-looking token
        for val in row:
            if not val: continue
            m = acc_re.search(val.strip().upper())
            if m:
                acc = m.group(1)
                if acc not in seen:
                    seen.add(acc); accs.append(acc)
                hits += 1
                break  # one accession per row is enough

os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w") as fo:
    for a in accs:
        fo.write(a + "\n")

with open(SUM, "w") as s:
    s.write(f"Source TSV: {TSV}\n")
    s.write(f"Header columns: {len(header)}\n")
    s.write("First columns: " + ", ".join(header[:6]) + ("\n" if header else "\n"))
    s.write(f"Rows scanned: {rows}\n")
    s.write(f"Rows with a detected accession (any field): {hits}\n")
    s.write(f"Unique accessions written: {len(accs)} -> {OUT}\n")

print(f"[done] unique accessions: {len(accs)}  (rows scanned: {rows})")
print(f"[summary] {SUM}")
