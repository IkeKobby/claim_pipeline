# Humaein Screening — Case Study #1: Claim Resubmission Ingestion Pipeline

This repository contains a simple, robust ingestion pipeline that:

- Ingests claim data from multiple EMR sources (CSV + JSON)
- Normalizes records to a unified schema
- Applies deterministic + heuristic logic to flag claims eligible for resubmission
- Produces `resubmission_candidates.json`
- Logs metrics in `metrics.json` and malformed records in `rejections.jsonl`

## Run locally

Requirements: Python 3.9+

```bash
python3 claim_pipeline.py
```

Optional arguments:

```bash
python3 claim_pipeline.py \
  --alpha data/emr_alpha.csv \
  --beta data/emr_beta.json \
  --out resubmission_candidates.json \
  --metrics metrics.json \
  --rejections rejections.jsonl
```

## Unified schema

Each normalized record conforms to:

```json
{
  "claim_id": "string",
  "patient_id": "string or null",
  "procedure_code": "string or null",
  "denial_reason": "string or null",
  "status": "approved|denied",
  "submitted_at": "YYYY-MM-DD (ISO date)",
  "source_system": "alpha|beta"
}
```

## Eligibility rules

Flag for resubmission if all are true:

1. `status == "denied"`
2. `patient_id` is present
3. `submitted_at` is more than 7 days before 2025-07-30
4. `denial_reason` matches known retryable reasons or is inferred retryable via heuristics

Retryable examples: `Missing modifier`, `Incorrect NPI`, `Prior auth required`.

Non-retryable examples: `Authorization expired`, `Incorrect provider type`.

Ambiguous are handled via a simple heuristic mapping (e.g., `Form incomplete` -> retryable; `Incorrect procedure` -> non-retryable).

## Outputs

- `resubmission_candidates.json` — list of candidates in the form:

```json
{
  "claim_id": "A124",
  "resubmission_reason": "Incorrect NPI",
  "source_system": "alpha",
  "recommended_changes": "Correct the NPI (rendering and/or billing) and resubmit"
}
```

- `metrics.json` — totals and exclusion reasons
- `rejections.jsonl` — one JSON object per malformed record

## Notes

- Solution is stdlib-only for portability.
- Extendable to add FastAPI or orchestration frameworks (Prefect/Dagster) if desired.


