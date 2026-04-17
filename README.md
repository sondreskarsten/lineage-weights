# lineage-weights

Nightly Cloud Run Job that scores each entity-lineage succession edge
with three continuity weightings:

1. **aksjonaer_score** — shareholder continuity via aksjeeierbok,
   top-20 holders aggregated by identity, L1 distance over aligned
   ownership percentages. 1.0 = identical cap table, 0.0 = disjoint.
2. **tillitsvalgt_score** — board/role continuity via roller_persons,
   weighted Jaccard on (person_id, role_code). SIGN and PROK are
   weighted heaviest (keep who can bind the company, keep the
   continuity signal). Note: roller history only covers Oct 2025+,
   so pre/post both read the current snapshot for now — score
   measures "governance survival to present", not true pre/post.
3. **regnskap_score** — balance-sheet continuity via finstat
   snapshots. `min(pre, post) / max(pre, post)` on SumEiendeler.
   Works back to 1989.

## Output

`gs://sondre_brreg_data/entity-lineage/weighted_edges.parquet`

Columns: predecessor_orgnr, successor_orgnr, event_date, event_type,
kid, confidence, aksjonaer_score, tillitsvalgt_score, regnskap_score,
plus audit inputs (pre/post top-20, pre/post eiendeler), plus
methodology_version and computed_at.

## Conventions

- Pre window: year(event_date) - 1
- Post window: year(event_date)
- methodology_version = "1.0.0" for this launch
- VPS/Euronext nominees excluded from aksjonaer scoring by orgnr
  list + name pattern match

## Deploy

```
gcloud run jobs execute lineage-weights --region europe-north1
```
