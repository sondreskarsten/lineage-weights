# Known Limitations — methodology_version 1.0.0

## Score coverage (first run, 2026-04-17)

| Score | Non-null | % | Source of sparsity |
|---|---|---|---|
| aksjonaer_score   |  8 462 |  8.5 % | aksjeeierbok is event-log, not state |
| tillitsvalgt_score |  1 137 |  1.1 % | roller history starts Oct 2025 |
| regnskap_score    | 74 693 | 74.7 % | finstat annual snapshots, good coverage |

## 1. aksjonaer: event-log misuse

`gs://sondre_brreg_data/aksjeeierbok/cdc/changelog/{yr}.parquet` contains
`new` / `modified` / `disappeared` events that occurred IN that year.
v1.0.0's `get_holders(orgnr, yr)` reads just one year's rows and treats
them as the cap table. This scores only companies whose cap table moved
in year-1 (pre) or year (post) of the succession. A company with a
stable cap table 2018-2020 produces zero rows for a 2019 fusion and
gets `aksjonaer_score = NA`.

### Fix for v2.0.0

Build a cumulative state reconstruction: for `(orgnr, asOf)`, collapse
every new/modified event with `valid_time <= asOf` per shareholder_id,
take the latest `curr_ownership_pct`. Disappeared events subtract.
Materialize as a single `aksjeeierbok/state/by_year/{yr}.parquet` at
year-end snapshots so scoring can join by `(orgnr, yr)`.

## 2. tillitsvalgt: no pre/post history

`roller/parsed/v1/role_persons/*.parquet` daily snapshots only go back
to 2025-10-21. For pre-2025 succession events, v1.0.0 reads the current
snapshot for both pre and post — which means score = 1.0 if both sides
still exist today and their person sets overlap, NA if either is gone.

### Fix for v2.0.0

Backfill historical roller snapshots from bulk JSON archives at
`gs://publicpannelbrreg/` (quarterly XBRL releases include the rolle
report). Or wait for 1-2 years of daily history and rescore.

## 3. Normalisation choices

- **Nominee exclusion**: hardcoded 5 orgnrs + name patterns (NOMINEE,
  FORVALTNING, SECURITIES OSLO, VERDIPAPIRSENTRALEN). May miss smaller
  nominee entities. Re-evaluate list quarterly.
- **Pre/post window**: year-1 vs year. For a December merger, pre-year
  reflects 11 months of pre-merger state, post-year reflects 1 month of
  post-merger + the effective closing date. Consider using
  `event_date ± 6 months` instead.
- **Top-20 holders**: hardcoded. For broadly-held companies (DNB etc.)
  this captures most of the cap table. For concentrated holdings
  (family-owned AS) it's overkill. Could scale N to N% total holders.

## Usage caveat

Consumers should treat `NA` as "insufficient data" not "score = 0".
The Registrum graph view should surface all three scores independently
so users can see which signals supported a given weighting.
