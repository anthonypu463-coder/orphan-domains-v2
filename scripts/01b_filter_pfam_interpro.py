#!/usr/bin/env python3
import csv, os, sys

SRC = "data/raw/uniprot_candidates.tsv"
DST = "data/interim/uniprot_candidates_noPF.tsv"
SUM = "data/interim/summary_filter_pfam_interpro.txt"

# Treat these as "no xref"
EMPTY_TOKENS = {"", "-", "na", "n/a", "null", "none"}

def norm(s: str) -> str:
    return (s or "").strip().lower()

def pick_columns(header):
    """
    Return (pf_key, ip_key). Try common UniProt TSV headers and fallbacks.
    """
    lh = [h.lower() for h in header]
    # direct matches first
    candidates = {
        "pfam": ["xref_pfam", "cross-reference (pfam)", "pfam"],
        "interpro": ["xref_interpro", "cross-reference (interpro)", "interpro"],
    }
    pf_key = next((h for h in header for pat in candidates["pfam"] if h.lower() == pat), None)
    ip_key = next((h for h in header for pat in candidates["interpro"] if h.lower() == pat), None)
    # substring fallback
    if pf_key is None:
        for h in header:
            if "pfam" in h.lower():
                pf_key = h; break
    if ip_key is None:
        for h in header:
            if "interpro" in h.lower():
                ip_key = h; break
    return pf_key, ip_key

def main():
    if not os.path.exists(SRC):
        print(f"ERROR: missing {SRC}", file=sys.stderr); sys.exit(2)

    os.makedirs(os.path.dirname(DST), exist_ok=True)

    total = 0
    kept = 0
    dropped = 0
    pf_key = ip_key = None

    with open(SRC, "r", newline="") as fin, open(DST, "w", newline="") as fout:
        r = csv.DictReader(fin, delimiter="\t")
        header = r.fieldnames or []
        pf_key, ip_key = pick_columns(header)
        if not pf_key or not ip_key:
            print(f"ERROR: could not locate Pfam/InterPro columns in header:\n{header}", file=sys.stderr)
            sys.exit(3)
        w = csv.DictWriter(fout, delimiter="\t", fieldnames=header)
        w.writeheader()

        for row in r:
            total += 1
            pf = norm(row.get(pf_key, "")) in EMPTY_TOKENS
            ip = norm(row.get(ip_key, "")) in EMPTY_TOKENS
            if pf and ip:
                w.writerow(row)
                kept += 1
            else:
                dropped += 1
            # progress every 100k
            if (total % 100000) == 0:
                print(f"[progress] processed={total:,} kept={kept:,} dropped={dropped:,}", flush=True)

    with open(SUM, "w") as f:
        f.write(f"Source: {SRC}\n")
        f.write(f"Output: {DST}\n")
        f.write(f"Pfam column used: {pf_key}\n")
        f.write(f"InterPro column used: {ip_key}\n")
        f.write(f"Total rows read (data only): {total}\n")
        f.write(f"Kept (no Pfam & no InterPro): {kept}\n")
        f.write(f"Dropped (had Pfam or InterPro): {dropped}\n")

    print(f"[done] kept={kept:,} dropped={dropped:,} total={total:,}")
    print(f"[summary] {SUM}")

if __name__ == "__main__":
    main()
