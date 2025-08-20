import argparse
import csv
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


REFERENCE_TODAY = date(2025, 7, 30)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def to_iso_date_string(raw: str) -> Optional[str]:
    if raw is None:
        return None
    raw_stripped = str(raw).strip()
    if raw_stripped == "":
        return None
    try:
        # Attempt full ISO datetime first
        dt = datetime.fromisoformat(raw_stripped)
        return dt.date().isoformat()
    except Exception:
        pass
    try:
        # Fallback to simple date
        dt = datetime.strptime(raw_stripped, "%Y-%m-%d")
        return dt.date().isoformat()
    except Exception:
        return None


def normalize_status(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    value = str(raw).strip().lower()
    if value in {"approved", "denied"}:
        return value
    return None


def normalize_string_or_none(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    value = str(raw).strip()
    if value.lower() in {"", "none", "null", "nan"}:
        return None
    return value


def days_between(submitted_at_iso_date: Optional[str], reference: date) -> Optional[int]:
    if submitted_at_iso_date is None:
        return None
    try:
        d = datetime.strptime(submitted_at_iso_date, "%Y-%m-%d").date()
        return (reference - d).days
    except Exception:
        return None


def canonicalize_denial_reason(raw: Optional[str]) -> Optional[str]:
    value = normalize_string_or_none(raw)
    if value is None:
        return None
    return value.strip()


def classify_denial_reason(raw_reason: Optional[str]) -> Tuple[bool, Optional[str], Optional[str]]:
    reason = canonicalize_denial_reason(raw_reason)
    if reason is None:
        return (False, None, "No denial reason provided; cannot determine if retryable")

    # Canonical mappings (case-insensitive comparisons)
    retryable_map: Dict[str, str] = {
        "missing modifier": "Missing modifier",
        "incorrect npi": "Incorrect NPI",
        "prior auth required": "Prior auth required",
    }
    non_retryable_map: Dict[str, str] = {
        "authorization expired": "Authorization expired",
        "incorrect provider type": "Incorrect provider type",
    }
    ambiguous_map: Dict[str, Tuple[bool, str]] = {
        # Heuristics for ambiguous reasons
        # format: lowercased_input -> (is_retryable, canonical_reason)
        "incorrect procedure": (False, "Incorrect procedure"),
        "form incomplete": (True, "Form incomplete"),
        "not billable": (False, "Not billable"),
    }

    recommended_changes: Dict[str, str] = {
        "Missing modifier": "Add the appropriate modifier based on payer rules and resubmit",
        "Incorrect NPI": "Correct the NPI (rendering and/or billing) and resubmit",
        "Prior auth required": "Obtain or attach proof of prior authorization and resubmit",
        "Authorization expired": "Obtain a new authorization; resubmit only if policy allows",
        "Incorrect provider type": "Verify provider taxonomy/role and adjust claim if applicable",
        "Incorrect procedure": "Verify the CPT/HCPCS code; correct coding prior to any resubmission",
        "Form incomplete": "Fill all required fields and attach missing documentation, then resubmit",
        "Not billable": "Review payer policy; consider alternative coding or an appeal",
    }

    key = reason.lower()
    if key in retryable_map:
        canonical = retryable_map[key]
        return (True, canonical, recommended_changes.get(canonical))
    if key in non_retryable_map:
        canonical = non_retryable_map[key]
        return (False, canonical, recommended_changes.get(canonical))
    if key in ambiguous_map:
        is_retryable, canonical = ambiguous_map[key]
        return (is_retryable, canonical, recommended_changes.get(canonical))

    # Default heuristic: treat unknown reasons as non-retryable but report
    return (False, reason, f"Unrecognized denial reason '{reason}'; defaulting to non-retryable")


@dataclass
class NormalizedClaim:
    claim_id: Optional[str]
    patient_id: Optional[str]
    procedure_code: Optional[str]
    denial_reason: Optional[str]
    status: Optional[str]  # "approved" | "denied"
    submitted_at: Optional[str]  # ISO date YYYY-MM-DD
    source_system: str


def normalize_alpha_row(row: Dict[str, str]) -> NormalizedClaim:
    return NormalizedClaim(
        claim_id=normalize_string_or_none(row.get("claim_id")),
        patient_id=normalize_string_or_none(row.get("patient_id")),
        procedure_code=normalize_string_or_none(row.get("procedure_code")),
        denial_reason=canonicalize_denial_reason(row.get("denial_reason")),
        status=normalize_status(row.get("status")),
        submitted_at=to_iso_date_string(row.get("submitted_at")),
        source_system="alpha",
    )


def normalize_beta_record(rec: Dict[str, object]) -> NormalizedClaim:
    return NormalizedClaim(
        claim_id=normalize_string_or_none(rec.get("id")),
        patient_id=normalize_string_or_none(rec.get("member")),
        procedure_code=normalize_string_or_none(rec.get("code")),
        denial_reason=canonicalize_denial_reason(rec.get("error_msg")),
        status=normalize_status(str(rec.get("status")) if rec.get("status") is not None else None),
        submitted_at=to_iso_date_string(str(rec.get("date")) if rec.get("date") is not None else None),
        source_system="beta",
    )


def read_alpha_csv(path: Path) -> Iterable[NormalizedClaim]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                yield normalize_alpha_row(row)
            except Exception as exc:
                logging.warning("Failed to normalize alpha row: %s | error=%s", row, exc)


def read_beta_json(path: Path) -> Iterable[NormalizedClaim]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        if not isinstance(data, list):
            logging.warning("Beta JSON is not a list; skipping")
            return
        for rec in data:
            if not isinstance(rec, dict):
                logging.warning("Beta JSON record is not an object: %s", rec)
                continue
            try:
                yield normalize_beta_record(rec)
            except Exception as exc:
                logging.warning("Failed to normalize beta record: %s | error=%s", rec, exc)


@dataclass
class PipelineMetrics:
    total_claims_processed: int = 0
    claims_by_source: Dict[str, int] = field(default_factory=lambda: {"alpha": 0, "beta": 0})
    eligible_for_resubmission: int = 0
    excluded_reasons_count: Dict[str, int] = field(
        default_factory=lambda: {
            "status_not_denied": 0,
            "missing_patient_id": 0,
            "too_recent": 0,
            "non_retryable_reason": 0,
            "malformed_record": 0,
        }
    )


def increment(metrics: PipelineMetrics, key: str) -> None:
    metrics.excluded_reasons_count[key] = metrics.excluded_reasons_count.get(key, 0) + 1


def is_eligible_for_resubmission(claim: NormalizedClaim, metrics: PipelineMetrics) -> Tuple[bool, Optional[str], Optional[str]]:
    # Step 1: status must be denied
    if claim.status != "denied":
        increment(metrics, "status_not_denied")
        return (False, None, None)

    # Step 2: patient_id must not be null
    if not claim.patient_id:
        increment(metrics, "missing_patient_id")
        return (False, None, None)

    # Step 3: submitted_at more than 7 days ago relative to REFERENCE_TODAY
    delta_days = days_between(claim.submitted_at, REFERENCE_TODAY)
    if delta_days is None or delta_days <= 7:
        increment(metrics, "too_recent")
        return (False, None, None)

    # Step 4: denial_reason is retryable or inferred retryable
    retryable, canonical_reason, suggestion = classify_denial_reason(claim.denial_reason)
    if not retryable:
        increment(metrics, "non_retryable_reason")
        return (False, canonical_reason, suggestion)

    return (True, canonical_reason, suggestion)


def process_claims(alpha_path: Path, beta_path: Path, 
                   out_path: Path, metrics_out_path: Optional[Path], 
                   rejections_log_path: Optional[Path]) -> None:
    metrics = PipelineMetrics()
    candidates: List[Dict[str, Optional[str]]] = []

    # Ensure rejections log file exists even if no malformed records are encountered
    if rejections_log_path is not None:
        rejections_log_path.parent.mkdir(parents=True, exist_ok=True)
        if not rejections_log_path.exists():
            with rejections_log_path.open("w", encoding="utf-8"):
                pass

    def handle_claim(claim: NormalizedClaim) -> None:
        metrics.total_claims_processed += 1
        metrics.claims_by_source[claim.source_system] = metrics.claims_by_source.get(claim.source_system, 0) + 1

        try:
            eligible, canonical_reason, suggestion = is_eligible_for_resubmission(claim, metrics)
        except Exception as exc:
            increment(metrics, "malformed_record")
            logging.error("Eligibility check failed for claim_id=%s source=%s error=%s", claim.claim_id, claim.source_system, exc)
            if rejections_log_path is not None:
                with rejections_log_path.open("a", encoding="utf-8") as rej:
                    rej.write(json.dumps({
                        "claim": claim.__dict__,
                        "error": str(exc),
                    }) + "\n")
            return

        if eligible:
            metrics.eligible_for_resubmission += 1
            candidates.append({
                "claim_id": claim.claim_id,
                "resubmission_reason": canonical_reason,
                "source_system": claim.source_system,
                "recommended_changes": suggestion,
            })

    # Read and handle claims from both sources
    if alpha_path.exists():
        for claim in read_alpha_csv(alpha_path):
            handle_claim(claim)
    else:
        logging.warning("Alpha source not found at %s", alpha_path)

    if beta_path.exists():
        for claim in read_beta_json(beta_path):
            handle_claim(claim)
    else:
        logging.warning("Beta source not found at %s", beta_path)

    # Save candidates
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(candidates, f, indent=2)
    logging.info("Saved %d resubmission candidates to %s", len(candidates), out_path)

    # Save metrics if requested
    metrics_dict = {
        "total_claims_processed": metrics.total_claims_processed,
        "claims_by_source": metrics.claims_by_source,
        "eligible_for_resubmission": metrics.eligible_for_resubmission,
        "excluded_reasons_count": metrics.excluded_reasons_count,
    }
    if metrics_out_path is not None:
        metrics_out_path.parent.mkdir(parents=True, exist_ok=True)
        with metrics_out_path.open("w", encoding="utf-8") as mf:
            json.dump(metrics_dict, mf, indent=2)
        logging.info("Saved metrics to %s", metrics_out_path)

    # Also print a concise metrics summary
    logging.info("Metrics summary: %s", json.dumps(metrics_dict))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Claim Resubmission Ingestion Pipeline")
    default_data_dir = Path(__file__).parent / "../data"
    parser.add_argument("--alpha", type=Path, default=default_data_dir / "emr_alpha.csv", help="Path to emr_alpha.csv")
    parser.add_argument("--beta", type=Path, default=default_data_dir / "emr_beta.json", help="Path to emr_beta.json")
    parser.add_argument("--out", type=Path, default=Path(__file__).parent / "resubmission_candidates.json", help="Output JSON file for candidates")
    parser.add_argument("--metrics", type=Path, default=Path(__file__).parent / "metrics.json", help="Output JSON file for metrics")
    parser.add_argument("--rejections", type=Path, default=Path(__file__).parent / "rejections.jsonl", help="JSONL log file for rejected/malformed records")
    return parser


def main() -> None:
    configure_logging()
    parser = build_arg_parser()
    args = parser.parse_args()

    process_claims(
        alpha_path=args.alpha,
        beta_path=args.beta,
        out_path=args.out,
        metrics_out_path=args.metrics,
        rejections_log_path=args.rejections,
    )


if __name__ == "__main__":
    main()


