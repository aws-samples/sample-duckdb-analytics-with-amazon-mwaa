#!/usr/bin/env python3
"""Generate deterministic, fictional AI inference telemetry for the blog.

The script writes Parquet files only. It does not contain customer data,
prompts, completions, user identifiers, AWS prices, or billing records.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


UTC = timezone.utc

MODELS = (
    "text-nano-v1",
    "text-standard-v1",
    "text-premium-v1",
    "reasoning-standard-v1",
    "embedding-small-v1",
    "embedding-large-v1",
)

APPLICATIONS = (
    "support-assistant",
    "document-summarizer",
    "knowledge-search",
    "developer-assistant",
    "claims-review",
    "product-enrichment",
    "research-workbench",
    "innovation-sandbox",
)

USE_CASES = (
    "question_answering",
    "summarization",
    "semantic_search",
    "code_assistance",
    "document_classification",
    "content_enrichment",
)

ENVIRONMENTS = ("production", "staging", "development")

EVENT_SCHEMA_V1 = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("application_id", pa.string()),
        ("model_id", pa.string()),
        ("input_tokens", pa.int64()),
        ("output_tokens", pa.int64()),
        ("latency_ms", pa.int32()),
        ("status_code", pa.int16()),
        ("context_json", pa.string()),
    ]
)

EVENT_SCHEMA_V2 = EVENT_SCHEMA_V1.append(
    pa.field("cached_input_tokens", pa.int64())
)

RATE_SCHEMA = pa.schema(
    [
        ("model_id", pa.string()),
        ("effective_from", pa.timestamp("us", tz="UTC")),
        ("input_credits_per_million", pa.float64()),
        ("output_credits_per_million", pa.float64()),
        ("cache_read_credits_per_million", pa.float64()),
    ]
)

OWNERSHIP_SCHEMA = pa.schema(
    [
        ("application_id", pa.string()),
        ("effective_from", pa.timestamp("us", tz="UTC")),
        ("team_id", pa.string()),
        ("cost_center", pa.string()),
        ("monthly_budget_credits", pa.float64()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate fictional AI inference showback data."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("sample_ai_inference"),
        help="Output directory. Default: sample_ai_inference",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2026, 9, 1),
        help="First ingest date in YYYY-MM-DD format. Default: 2026-09-01",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of daily partitions. Default: 30",
    )
    parser.add_argument(
        "--events-per-day",
        type=int,
        default=10_000,
        help="Base events per day before duplicates. Default: 10000",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed. Default: 42",
    )
    return parser.parse_args()


def deterministic_id(seed: int, ordinal: int) -> str:
    value = f"{seed}:{ordinal}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()[:32]


def midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=UTC)


def write_parquet(
    rows: list[dict[str, Any]], schema: pa.Schema, destination: Path
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(
        table,
        destination,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )


def token_counts(rng: random.Random, model_id: str) -> tuple[int, int]:
    if model_id.startswith("embedding"):
        return rng.randint(64, 2_048), 0
    input_tokens = max(32, int(rng.lognormvariate(6.4, 0.8)))
    output_tokens = max(8, int(rng.lognormvariate(5.2, 0.9)))
    return min(input_tokens, 32_000), min(output_tokens, 8_000)


def make_event(
    rng: random.Random,
    seed: int,
    ordinal: int,
    ingest_day: date,
    include_cache_field: bool,
) -> dict[str, Any]:
    model_id = rng.choice(MODELS)
    application_id = rng.choice(APPLICATIONS)
    input_tokens, output_tokens = token_counts(rng, model_id)

    event_day = ingest_day
    if rng.random() < 0.04:
        event_day -= timedelta(days=rng.choice((1, 1, 2)))

    event_ts = midnight(event_day) + timedelta(
        seconds=rng.randrange(86_400),
        microseconds=rng.randrange(1_000_000),
    )
    ingestion_delay = timedelta(
        seconds=rng.randint(5, 1_800),
        microseconds=rng.randrange(1_000_000),
    )
    if event_day < ingest_day:
        ingestion_delay += midnight(ingest_day) - midnight(event_day)

    status_code = rng.choices(
        (200, 400, 429, 500), weights=(94, 2, 3, 1), k=1
    )[0]
    use_case = rng.choice(USE_CASES)
    environment = rng.choices(
        ENVIRONMENTS, weights=(75, 15, 10), k=1
    )[0]
    context = {
        "use_case": use_case,
        "environment": environment,
        "priority": rng.choice(("standard", "standard", "batch", "urgent")),
        "project": f"project-{rng.randint(1, 24):02d}",
    }

    row: dict[str, Any] = {
        "event_id": deterministic_id(seed, ordinal),
        "event_ts": event_ts,
        "ingested_at": event_ts + ingestion_delay,
        "application_id": application_id,
        "model_id": model_id,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "latency_ms": max(20, int(rng.lognormvariate(5.7, 0.65))),
        "status_code": status_code,
        "context_json": json.dumps(context, separators=(",", ":"), sort_keys=True),
    }
    if include_cache_field:
        cache_ratio = rng.choice((0.0, 0.0, 0.25, 0.5, 0.75))
        row["cached_input_tokens"] = int(input_tokens * cache_ratio)
    return row


def generate_events(
    output: Path,
    start_date: date,
    days: int,
    events_per_day: int,
    seed: int,
) -> tuple[int, int, date]:
    rng = random.Random(seed)
    schema_change_day = start_date + timedelta(days=max(1, days // 2))
    total_rows = 0
    duplicate_rows = 0
    ordinal = 0

    for day_offset in range(days):
        ingest_day = start_date + timedelta(days=day_offset)
        include_cache_field = ingest_day >= schema_change_day
        rows: list[dict[str, Any]] = []

        for _ in range(events_per_day):
            row = make_event(
                rng,
                seed,
                ordinal,
                ingest_day,
                include_cache_field,
            )
            rows.append(row)
            ordinal += 1

            if rng.random() < 0.012:
                duplicate = dict(row)
                duplicate["ingested_at"] = row["ingested_at"] + timedelta(
                    minutes=rng.randint(1, 30)
                )
                duplicate["latency_ms"] = max(
                    20, row["latency_ms"] + rng.randint(-20, 80)
                )
                rows.append(duplicate)
                duplicate_rows += 1

        destination = (
            output
            / "inference_events"
            / f"ingest_date={ingest_day.isoformat()}"
            / "part-000.parquet"
        )
        schema = EVENT_SCHEMA_V2 if include_cache_field else EVENT_SCHEMA_V1
        write_parquet(rows, schema, destination)
        total_rows += len(rows)

    return total_rows, duplicate_rows, schema_change_day


def generate_model_rates(
    output: Path, start_date: date, days: int
) -> int:
    midpoint = start_date + timedelta(days=max(1, days // 2))
    base_rates = {
        "text-nano-v1": (0.10, 0.40, 0.03),
        "text-standard-v1": (0.60, 2.40, 0.12),
        "text-premium-v1": (2.50, 10.00, 0.50),
        "reasoning-standard-v1": (1.50, 6.00, 0.30),
        "embedding-small-v1": (0.05, 0.00, 0.01),
        "embedding-large-v1": (0.18, 0.00, 0.04),
    }
    rows: list[dict[str, Any]] = []

    for model_id, rates in base_rates.items():
        initial_date = start_date
        if model_id == "reasoning-standard-v1":
            initial_date += timedelta(days=2)
        rows.append(
            {
                "model_id": model_id,
                "effective_from": midnight(initial_date),
                "input_credits_per_million": rates[0],
                "output_credits_per_million": rates[1],
                "cache_read_credits_per_million": rates[2],
            }
        )
        rows.append(
            {
                "model_id": model_id,
                "effective_from": midnight(midpoint),
                "input_credits_per_million": round(rates[0] * 0.92, 6),
                "output_credits_per_million": round(rates[1] * 0.95, 6),
                "cache_read_credits_per_million": round(rates[2] * 0.90, 6),
            }
        )

    destination = output / "reference" / "model_rates.parquet"
    write_parquet(rows, RATE_SCHEMA, destination)
    return len(rows)


def generate_ownership(
    output: Path, start_date: date, days: int
) -> int:
    midpoint = start_date + timedelta(days=max(1, days // 2))
    ownership = {
        "support-assistant": ("customer-experience", "CC-101", 900.0),
        "document-summarizer": ("productivity", "CC-201", 500.0),
        "knowledge-search": ("enterprise-search", "CC-301", 700.0),
        "developer-assistant": ("developer-platform", "CC-401", 1_100.0),
        "claims-review": ("operations", "CC-501", 650.0),
        "product-enrichment": ("digital-commerce", "CC-601", 800.0),
        "research-workbench": ("research", "CC-701", 450.0),
        "innovation-sandbox": ("innovation", "CC-801", 250.0),
    }
    rows: list[dict[str, Any]] = []

    for application_id, values in ownership.items():
        effective_date = start_date
        if application_id == "innovation-sandbox":
            effective_date += timedelta(days=5)
        rows.append(
            {
                "application_id": application_id,
                "effective_from": midnight(effective_date),
                "team_id": values[0],
                "cost_center": values[1],
                "monthly_budget_credits": values[2],
            }
        )

    rows.append(
        {
            "application_id": "knowledge-search",
            "effective_from": midnight(midpoint),
            "team_id": "data-platform",
            "cost_center": "CC-302",
            "monthly_budget_credits": 750.0,
        }
    )

    destination = output / "reference" / "application_ownership.parquet"
    write_parquet(rows, OWNERSHIP_SCHEMA, destination)
    return len(rows)


def main() -> None:
    args = parse_args()
    if args.days < 1:
        raise SystemExit("--days must be at least 1")
    if args.events_per_day < 1:
        raise SystemExit("--events-per-day must be at least 1")

    args.output.mkdir(parents=True, exist_ok=True)
    event_rows, duplicate_rows, schema_change_day = generate_events(
        args.output,
        args.start_date,
        args.days,
        args.events_per_day,
        args.seed,
    )
    rate_rows = generate_model_rates(args.output, args.start_date, args.days)
    ownership_rows = generate_ownership(
        args.output, args.start_date, args.days
    )

    manifest = {
        "dataset": "fictional_ai_inference_showback",
        "seed": args.seed,
        "start_date": args.start_date.isoformat(),
        "days": args.days,
        "events_per_day": args.events_per_day,
        "event_rows_written": event_rows,
        "duplicate_rows": duplicate_rows,
        "unique_event_ids": event_rows - duplicate_rows,
        "model_rate_rows": rate_rows,
        "ownership_rows": ownership_rows,
        "schema_change_day": schema_change_day.isoformat(),
        "contains_customer_data": False,
        "contains_prompts_or_completions": False,
        "contains_aws_prices": False,
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
