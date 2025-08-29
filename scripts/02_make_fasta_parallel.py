#!/usr/bin/env python3
import os, csv, time, sys, threading, concurrent.futures, requests

# Inputs/outputs
TSV = "data/interim/uniprot_candidates_noPF.tsv"
OUT_FASTA = "data/raw/candidates_filtered.fasta"
OK_LIST = "data/raw/downloaded_accessions.txt"     # successes (for resume)
FAIL_LIST = "data/raw/failed_accessions.txt"       # failures summary

# Settings
N_WORKERS = 8
SIZE_HINT = 100  # print progress every this many successes
URL_TPL = "https://rest.uniprot.org/uniprotkb/{acc}.fasta"
HEADERS = {
    "User-Agent": "orphan-domains-v2/0.1",
    "Accept": "text/plain",
    "Accept-Encoding": "gzip, deflate",
}

def detect_acc_col(path):
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        hdr = r.fieldnames or []
        for cand in ("accession", "Accession", "Entry", "entry"):
            if cand in hdr: return cand
        # fallback: first column if it looks accession-like in first row
        row = next(r, None)
        if row:
            first_key = hdr[0]
            val = (row.get(first_key) or "").strip()
            if val and val[0].isalnum():
                return first_key
    raise SystemExit(f"ERROR: accession column not found in {path} (header={hdr})")

def load_accessions(path, col):
    accs = []
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            a = (row.get(col) or "").strip()
            if a: accs.append(a)
    # unique, stable order
    seen = set(); out = []
    for a in accs:
        if a not in seen:
            seen.add(a); out.append(a)
    return out

def load_set(path):
    s=set()
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                s.add(line.strip())
    return s

def fetch_one(acc):
    # per-thread session for safety
    sess = requests.Session()
    for attempt in range(1, 7):  # up to 6 tries
        try:
            resp = sess.get(URL_TPL.format(acc=acc), headers=HEADERS, timeout=(10, 30))
            if resp.status_code == 200 and resp.text.startswith(">"):
                text = resp.text
                if not text.endswith("\n"): text += "\n"
                return acc, text
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2*attempt, 10))
                continue
            # hard failure
            return acc, None
        except Exception:
            time.sleep(min(2*attempt, 10))
    return acc, None

def main():
    if not os.path.exists(TSV):
        raise SystemExit(f"ERROR: missing {TSV}")
    os.makedirs(os.path.dirname(OUT_FASTA), exist_ok=True)

    acc_col = detect_acc_col(TSV)
    accs = load_accessions(TSV, acc_col)

    ok = load_set(OK_LIST)
    # if resuming and fasta exists, append; else write fresh
    mode = "a" if (ok and os.path.exists(OUT_FASTA)) else "w"
    written_start = len(ok)

    lock = threading.Lock()
    total_ok = written_start
    total_fail = 0
    to_do = [a for a in accs if a not in ok]

    print(f"[init] accessions={len(accs)}  already_ok={written_start}  to_fetch={len(to_do)}  col={acc_col}")
    if not to_do:
        print("[done] nothing to do; all accessions already downloaded")
        return

    with open(OUT_FASTA, mode) as fout, open(OK_LIST, "a") as fok:
        with concurrent.futures.ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
            for acc, fasta in ex.map(fetch_one, to_do):
                with lock:
                    if fasta:
                        fout.write(fasta)
                        fok.write(acc + "\n")
                        total_ok += 1
                        if total_ok % SIZE_HINT == 0:
                            print(f"[progress] ok={total_ok} fail={total_fail}")
                    else:
                        total_fail += 1
                        # accumulate failures; write at end
    if total_fail:
        with open(FAIL_LIST, "w") as ff:
            # recompute failures as those not in OK
            remaining = [a for a in accs if a not in load_set(OK_LIST)]
            for a in remaining:
                ff.write(a + "\n")

    print(f"[done] wrote_ok={total_ok}  failed={total_fail}  fasta={OUT_FASTA}")
    if total_fail:
        print(f"[note] failures listed in {FAIL_LIST}")

if __name__ == "__main__":
    main()
