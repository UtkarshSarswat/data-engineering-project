import argparse
import json
import random
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np


EVENT_TYPES = ["page_view", "click", "signup", "purchase"]
EVENT_WEIGHTS = [0.58, 0.27, 0.10, 0.05]
DEVICE_TYPES = ["mobile", "web", "tablet"]
DEVICE_WEIGHTS = [0.57, 0.38, 0.05]
COUNTRIES = ["US", "IN", "GB", "DE", "CA", "AU", "BR", "SG", "FR", "JP"]
COUNTRY_WEIGHTS = [0.30, 0.18, 0.10, 0.09, 0.08, 0.06, 0.06, 0.05, 0.04, 0.04]
PAGES = ["/", "/pricing", "/products", "/cart", "/checkout", "/docs", "/account"]
PRODUCTS = [f"sku_{i:04d}" for i in range(1, 501)]


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic user activity events.")
    parser.add_argument("--rows", type=int, default=500_000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--clean", action="store_true", help="Remove the output directory first.")
    return parser.parse_args()


def weighted_choice(values, weights):
    return random.choices(values, weights=weights, k=1)[0]


def build_payload(event_type):
    payload = {
        "page": random.choice(PAGES),
        "referrer": random.choice(["search", "email", "ad", "direct", "social"]),
    }
    if event_type in {"click", "purchase"}:
        payload["product_id"] = random.choice(PRODUCTS)
    if event_type == "purchase":
        payload["value"] = round(float(np.random.gamma(shape=2.0, scale=45.0) + 8), 2)
    return payload


def generate_base_events(rows, days):
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days)
    total_seconds = int((end_date - start_date).total_seconds())

    events = []
    for _ in range(rows):
        offset = random.randint(0, total_seconds - 1)
        event_time = start_date + timedelta(seconds=offset)
        event_type = weighted_choice(EVENT_TYPES, EVENT_WEIGHTS)
        event_id = str(uuid.uuid4())
        user_id = f"user_{random.randint(1, 65_000):06d}"
        session_id = f"sess_{random.randint(1, 180_000):06d}"

        events.append(
            {
                "event_id": event_id,
                "user_id": user_id,
                "event_type": event_type,
                "event_category": "transactional" if event_type == "purchase" else "engagement",
                "event_timestamp": event_time.isoformat().replace("+00:00", "Z"),
                "session_id": session_id,
                "country_code": weighted_choice(COUNTRIES, COUNTRY_WEIGHTS),
                "device_type": weighted_choice(DEVICE_TYPES, DEVICE_WEIGHTS),
                "is_bot": random.random() < 0.045,
                "payload": build_payload(event_type),
                "_source_date": event_time.date().isoformat(),
            }
        )
    return events


def inject_quality_issues(events, malformed_rate=0.01, null_rate=0.03, duplicate_rate=0.02):
    row_count = len(events)

    for idx in random.sample(range(row_count), int(row_count * null_rate)):
        field = random.choice(["country_code", "device_type", "event_category"])
        events[idx][field] = None

    for idx in random.sample(range(row_count), int(row_count * malformed_rate)):
        events[idx]["event_timestamp"] = random.choice(["bad_timestamp", "2026-99-99T99:99:99Z", "not-a-date"])

    duplicates = []
    for idx in random.sample(range(row_count), int(row_count * duplicate_rate)):
        duplicate = dict(events[idx])
        duplicate["payload"] = dict(events[idx]["payload"])
        duplicates.append(duplicate)

    return events + duplicates


def write_jsonl_partitions(events, output_dir):
    output_path = Path(output_dir)
    grouped = {}
    for event in events:
        source_date = event.pop("_source_date")
        grouped.setdefault(source_date, []).append(event)

    output_path.mkdir(parents=True, exist_ok=True)
    for date, rows in sorted(grouped.items()):
        part_dir = output_path / f"date={date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        with (part_dir / "events.jsonl").open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")

    return {date: len(rows) for date, rows in grouped.items()}


def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)

    output_path = Path(args.output_dir)
    if args.clean and output_path.exists():
        shutil.rmtree(output_path)

    events = generate_base_events(args.rows, args.days)
    events = inject_quality_issues(events)
    partition_counts = write_jsonl_partitions(events, output_path)

    print(f"wrote {sum(partition_counts.values())} rows across {len(partition_counts)} partitions to {output_path}")


if __name__ == "__main__":
    main()
