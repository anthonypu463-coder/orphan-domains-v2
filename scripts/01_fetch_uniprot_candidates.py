#!/usr/bin/env python3
import csv, time, os, sys, requests

os.makedirs("data/raw", exist_ok=True)
OUT = "data/raw/uniprot_candidates.tsv"

URL = "https://rest.uniprot.org/uniprotkb/search"
SIZE = 200  # smaller pages fetch faster & print more often
FIELDS = ",".join([
    "accession","id","protein_name","organism_name","length",
    "go_id","ec","xref_pfam","xref_interpro"
])
QUERY = (
    'reviewed:false AND fragment:false AND ('
    'protein_name:"uncharacterized protein" OR '
    'protein_name:"uncharacterised protein" OR '
    'protein_name:"hypothetical protein")'
)

HEADERS = {
    "User-Agent": "orphan-domains-v2/0.1 (contact: none)",
    "Accept": "text/tab-separated-values",
    "Accept-Encoding": "gzip, deflate",
}

def fetch_page(sess, offset):
    params = {"query": QUERY, "format": "tsv", "fields": FIELDS, "size": SIZE, "offset": offset}
    for attempt in range(1, 8):  # up to 7 tries
        try:
            print(f"[try] offset={offset} attempt={attempt}", flush=True)
            r = sess.get(URL, params=params, headers=HEADERS, timeout=(10, 25), allow_redirects=True)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2 * attempt, 10))
                continue
            r.raise_for_status()
            txt = r.text.strip().splitlines()
            return txt
        except Exception as e:
            if attempt == 7:
                print(f"[error] offset={offset} giving up: {e}", file=sys.stderr, flush=True)
                raise
            time.sleep(min(2 * attempt, 10))
    return []

def main():
    sess = requests.Session()
    total = 0
    pages = 0
    offset = 0
    with open(OUT, "w", newline="") as fout:
        writer = None
        while True:
            lines = fetch_page(sess, offset)
            if not lines:
                break
            header, rows = lines[0].split("\t"), lines[1:]
            if writer is None:
                writer = csv.writer(fout, delimiter="\t")
                writer.writerow(header)
            for row in rows:
                writer.writerow(row.split("\t"))
            pages += 1
            total += len(rows)
            print(f"[page] pages={pages} offset={offset} added={len(rows)} total={total}", flush=True)
            if len(rows) < SIZE:
                break  # last page
            offset += SIZE
            time.sleep(0.1)
    print(f"[done] wrote {total} rows -> {OUT}", flush=True)

if __name__ == "__main__":
    main()
