# Registered Criteria — Orphan-Domains v2 (fresh start)

## Tiers
- **Core**: strict orphans for headline results
- **Plus**: near-orphans (borderline quality) flagged

## Operational orphan rule
- UniProtKB: reviewed:false AND fragment:false AND name contains
  “uncharacterized/uncharacterised/hypothetical”
- Drop entries with Pfam or InterPro xrefs at fetch time

## Sequence screens (MMseqs2)
- Against Swiss-Prot and UniRef90
- Remove if identity ≥ 0.30 AND query coverage ≥ 0.70
- Use: `--cov-mode 2 -c 0.7 --min-seq-id 0.3` (report qcov & tcov)

## Segmentation & quality
- pLDDT-linker segmentation, then **PAE-aware adjustment** (shift ≤ ±15 aa;
  score = CrossPAE − mean(IntraPAE_L, IntraPAE_R)); min domain length 50 aa
- Core requires mean pLDDT ≥ 60 (Plus may allow 55–60, flagged)
- Record `boundary_method ∈ {plddt, plddt+pae, dpam}` + boundary metrics

## Boundary validation (DPAM)
- Stratified sample (~300); compute IoU vs our segments
- If median IoU < 0.60 → adopt DPAM for discordant parents (or globally)

## Structural novelty (Foldseek)
- Search vs PDB/CATH/ECOD (≥1 DB), format-mode 4
- **Non-novel** if E ≤ 1e-3 AND qcov ≥ 0.5 AND tcov ≥ 0.5; else novel
- Report novelty % overall / by clade / by length bin

## Redundancy & splits
- Collapse redundancy (e.g., linclust on domain seqs)
- Parent grouping: a UniProt accession never appears across multiple splits
- Splits are homology- and structure-clean

## Determinism & FAIR
- Fixed seeds; record tool versions & AF/UniProt release dates
- Code: MIT; Data: CC BY 4.0; include dataset.json, CITATION.cff, checksums
