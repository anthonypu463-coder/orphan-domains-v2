#!/usr/bin/env python3
import csv, os, re, sys

RAW = "data/raw/uniprot_candidates.tsv"
OUT = "data/interim/uniprot_candidates_noPF.tsv"
ACC_OUT = "data/interim/accessions.txt"
SUM = "data/interim/summary_filter_noPF_clean.txt"

EMPTY_TOKENS = {"", "-", "na", "n/a", "null", "none", "[]", '""', "''"}

def norm(s: str) -> str:
    s = (s or "").strip()
    # strip common wrappers/brackets and extra spaces
    s = s.strip('[]"\' \t\r\n')
    return s.lower()

def find_col(header, names, substr=None):
    lh = [h.lower() for h in header]
    for n in names:
        # exact (case-insensitive)
        if n.lower() in lh:
            return header[lh.index(n.lower())]
    if substr:
        for h in header:
            if substr.lower() in h.lower():
                return h
    return None

def is_accession(val: str) -> bool:
    v = (val or "").strip().upper()
    # primary patterns (6–10 alnum) with optional -isoform
    return bool(re.match(r'^[A-Z0-9]{6,10}(?:-\d+)?$', v))

def acc_from_entryname(v: str):
    # e.g., Q3V4A4_MOUSE -> Q3V4A4
    if not v: return None
    v = v.strip().upper()
    if "_" in v:
        v = v.split("_", 1)[0]
    return v if is_accession(v) else None

if not os.path.exists(RAW):
    print(f"ERROR: missing {RAW}", file=sys.stderr); sys.exit(2)

kept = dropped = 0
seen_acc = set()
rows_out = []

with open(RAW, newline="") as fin:
    r = csv.DictReader(fin, delimiter="\t")
    header = r.fieldnames or []
    if not header:
        print("ERROR: empty header", file=sys.stderr); sys.exit(3)

    pf_col = find_col(header, ["xref_pfam","pfam","cross-reference (pfam)"], substr="pfam")
    ip_col = find_col(header, ["xref_interpro","interpro","cross-reference (interpro)"], substr="interpro")
    acc_col = find_col(header, ["Entry","Accession","accession","Primary (Accession number)"])
    ename_col = find_col(header, ["Entry Name","entry name"])

    if not pf_col or not ip_col:
        print("ERROR: could not locate Pfam/InterPro columns", file=sys.stderr); sys.exit(4)
    if not acc_col and not ename_col:
        print("ERROR: could not locate accession or entry-name column", file=sys.stderr); sys.exit(5)

    for row in r:
        pf = norm(row.get(pf_col, ""))
        ip = norm(row.get(ip_col, ""))
        pf_empty = pf in EMPTY_TOKENS
        ip_empty = ip in EMPTY_TOKENS
        if not (pf_empty and ip_empty):
            dropped += 1
            continue

        acc = None
        if acc_col:
            cand = (row.get(acc_col) or "").strip()
            if is_accession(cand):
                acc = cand.strip().upper()
        if not acc and ename_col:
            acc = acc_from_entryname(row.get(ename_col))

        # if still none, try to scan all fields for a token that looks like an accession
        if not acc:
            for v in row.values():
                v = (v or "").strip().upper()
                m = re.search(r'[A-Z0-9]{6,10}(?:-\d+)?', v)
                if m and is_accession(m.group(0)):
                    acc = m.group(0); break

        if not acc:
            # cannot identify an accession; skip this row to be safe
            dropped += 1
            continue

        if acc in seen_acc:
            continue
        seen_acc.add(acc)
        kept += 1
        rows_out.append((row, acc))

# Write filtered TSV with original header
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w", newline="") as fout:
    w = csv.DictWriter(fout, delimiter="\t", fieldnames=header)
    w.writeheader()
    for row, _acc in rows_out:
        w.writerow(row)

# Write accession list
with open(ACC_OUT, "w") as fa:
    for _row, acc in rows_out:
        fa.write(acc + "\n")

with open(SUM, "w") as s:
    s.write(f"Source: {RAW}\n")
    s.write(f"Header columns: {len(header)}\n")
    s.write(f"Using Pfam column: {pf_col}\n")
    s.write(f"Using InterPro column: {ip_col}\n")
    s.write(f"Accession column: {acc_col or '(via Entry Name / scan)'}\n")
    s.write(f"Kept (no Pfam & no InterPro): {kept}\n")
    s.write(f"Dropped: {dropped}\n")
    s.write(f"Unique accessions written: {len(seen_acc)} -> {ACC_OUT}\n")

print(f"[done] kept={kept} dropped={dropped} unique_acc={len(seen_acc)}")
print(f"[out] TSV={OUT}")
print(f"[out] ACC_LIST={ACC_OUT}")
print(f"[summary] {SUM}")
