# Graph Schema — Orphan-Domains v2

**Nodes (per residue, Cα)**
- Features (order): 21-dim AA one-hot (20+X), pLDDT/100, index_norm,
  hydropathy, charge(+/−). Optional: φ/ψ.

**Edges**
- Sequential: (i,i+1), is_seq=1, distance=Cα–Cα (Å)
- Spatial: kNN (k=16) or radius (R=8Å), is_seq=0, distance in Å

**Tensors**
- x [N,F] float32; edge_index [2,E] long; edge_attr [E,2] float32 ([is_seq, distance Å]);
  pos [N,3] float32 (Å)

**IDs & metadata**
- domain_id = UNIPROTACC_start_end; CSVs align on domain_id; units = Å
