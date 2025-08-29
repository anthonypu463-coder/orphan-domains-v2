#!/usr/bin/env python3
import os, sys, time, threading, concurrent.futures, requests

LIST = "data/interim/accessions.txt"
OUT_FASTA = "data/raw/candidates_filtered.fasta"
OK_LIST = "data/raw/downloaded_accessions.txt"
FAIL_LIST = "data/raw/failed_accessions.txt"

N_WORKERS = 8
PROG_EVERY = 100
URL_TPL = "https://rest.uniprot.org/uniprotkb/{acc}.fasta"
HEADERS = {"User-Agent":"orphan-domains-v2/0.1","Accept":"text/plain","Accept-Encoding":"gzip, deflate"}

def load_list(p):
    with open(p) as f:
        return [line.strip() for line in f if line.strip()]

def load_set(p):
    s=set()
    if os.path.exists(p):
        with open(p) as f:
            for line in f: s.add(line.strip())
    return s

def fetch_one(acc):
    sess = requests.Session()
    for attempt in range(1,7):
        try:
            r = sess.get(URL_TPL.format(acc=acc), headers=HEADERS, timeout=(10,30))
            if r.status_code==200 and r.text.startswith(">"):
                t=r.text
                if not t.endswith("\n"): t+="\n"
                return acc, t
            if r.status_code in (429,500,502,503,504):
                time.sleep(min(2*attempt,10)); continue
            return acc, None
        except Exception:
            time.sleep(min(2*attempt,10))
    return acc, None

def main():
    if not os.path.exists(LIST):
        print(f"ERROR: missing {LIST}", file=sys.stderr); sys.exit(2)
    os.makedirs(os.path.dirname(OUT_FASTA), exist_ok=True)

    accs = load_list(LIST)
    ok = load_set(OK_LIST)
    to_do = [a for a in accs if a not in ok]

    mode = "a" if (ok and os.path.exists(OUT_FASTA)) else "w"
    total_ok = len(ok)
    total_fail = 0
    lock = threading.Lock()

    print(f"[init] accessions={len(accs)} already_ok={len(ok)} to_fetch={len(to_do)}")
    if not to_do:
        print("[done] nothing to do"); return

    with open(OUT_FASTA, mode) as fout, open(OK_LIST,"a") as fok:
        with concurrent.futures.ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
            for acc, fasta in ex.map(fetch_one, to_do):
                with lock:
                    if fasta:
                        fout.write(fasta); fok.write(acc+"\n"); total_ok+=1
                        if total_ok % PROG_EVERY == 0:
                            print(f"[progress] ok={total_ok} fail={total_fail}")
                    else:
                        total_fail+=1

    if total_fail:
        remaining=[a for a in accs if a not in load_set(OK_LIST)]
        with open(FAIL_LIST,"w") as ff:
            for a in remaining: ff.write(a+"\n")

    print(f"[done] wrote_ok={total_ok} failed={total_fail} fasta={OUT_FASTA}")
    if total_fail: print(f"[note] failures listed in {FAIL_LIST}")

if __name__ == "__main__":
    main()
