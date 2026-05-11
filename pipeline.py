import argparse
import json
import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


REQUIRED_COLUMNS = [
    "event_id",
    "user_id",
    "event_type",
    "event_category",
    "event_timestamp",
    "session_id",
    "country_code",
    "device_type",
    "is_bot",
    "payload",
]
EVENT_TYPES = {"page_view", "click", "signup", "purchase"}
DEVICE_TYPES = {"mobile", "web", "tablet"}


def parse_args():
    parser = argparse.ArgumentParser(description="Run the batch event pipeline.")
    parser.add_argument("--input-dir", default="data/raw")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--manifest-path", default="output/pipeline_manifest.json")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def configure_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def read_jsonl_inputs(input_dir):
    files = sorted(Path(input_dir).glob("date=*/events.jsonl"))
    if not files:
        raise FileNotFoundError(f"no input files found under {input_dir}")

    frames = []
    for file_path in files:
        frames.append(pd.read_json(file_path, lines=True))
    return pd.concat(frames, ignore_index=True), files


def extract_payload_fields(df):
    payload = pd.json_normalize(df["payload"]).reindex(columns=["page", "referrer", "product_id", "value"])
    df = df.drop(columns=["payload"]).join(payload)
    df["purchase_value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)
    df = df.drop(columns=["value"])
    return df


def session_duration_bucket(minutes):
    if minutes <= 5:
        return "short"
    if minutes <= 30:
        return "medium"
    return "long"


def classify_sessions(df):
    session_times = (
        df.groupby("session_id", as_index=False)
        .agg(session_start=("event_ts", "min"), session_end=("event_ts", "max"))
    )
    session_times["session_duration_minutes"] = (
        (session_times["session_end"] - session_times["session_start"]).dt.total_seconds() / 60
    ).round(2)
    session_times["session_duration_bucket"] = session_times["session_duration_minutes"].apply(session_duration_bucket)
    return df.merge(
        session_times[["session_id", "session_duration_minutes", "session_duration_bucket"]],
        on="session_id",
        how="left",
    )


def clean_and_transform(raw):
    report = {
        "input_rows": int(len(raw)),
        "missing_required_columns": [],
        "malformed_timestamp": 0,
        "duplicate_event_id": 0,
        "bot_traffic": 0,
        "invalid_event_type": 0,
        "invalid_device_type": 0,
        "null_country_code_flagged": 0,
        "null_device_type_dropped": 0,
    }

    missing = [column for column in REQUIRED_COLUMNS if column not in raw.columns]
    if missing:
        report["missing_required_columns"] = missing
        raise ValueError(f"input is missing required columns: {missing}")

    df = raw.copy()
    df["event_ts"] = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True)

    bad_timestamp = df["event_ts"].isna()
    report["malformed_timestamp"] = int(bad_timestamp.sum())
    bad_records = df.loc[bad_timestamp].copy()
    bad_records["bad_record_reason"] = "malformed_timestamp"
    df = df.loc[~bad_timestamp].copy()

    duplicate_mask = df.duplicated("event_id", keep="first")
    report["duplicate_event_id"] = int(duplicate_mask.sum())
    df = df.loc[~duplicate_mask].copy()

    bot_mask = df["is_bot"].fillna(False).astype(bool)
    report["bot_traffic"] = int(bot_mask.sum())
    df = df.loc[~bot_mask].copy()

    invalid_event = ~df["event_type"].isin(EVENT_TYPES)
    report["invalid_event_type"] = int(invalid_event.sum())
    df = df.loc[~invalid_event].copy()

    invalid_device = df["device_type"].notna() & ~df["device_type"].isin(DEVICE_TYPES)
    report["invalid_device_type"] = int(invalid_device.sum())
    null_device = df["device_type"].isna()
    report["null_device_type_dropped"] = int(null_device.sum())
    df = df.loc[~invalid_device & ~null_device].copy()

    report["null_country_code_flagged"] = int(df["country_code"].isna().sum())
    df["country_code"] = df["country_code"].fillna("UNKNOWN")
    df["event_category"] = df["event_category"].fillna("unknown")

    df = extract_payload_fields(df)
    df["date"] = df["event_ts"].dt.date.astype(str)
    df["hour"] = df["event_ts"].dt.hour.astype("int16")
    df = classify_sessions(df)
    df = df.sort_values(["user_id", "date", "event_ts", "event_id"])
    df["event_sequence_number"] = df.groupby(["user_id", "date"]).cumcount() + 1

    cleaned_columns = [
        "event_id",
        "user_id",
        "event_type",
        "event_category",
        "event_ts",
        "date",
        "hour",
        "session_id",
        "country_code",
        "device_type",
        "page",
        "referrer",
        "product_id",
        "purchase_value",
        "session_duration_minutes",
        "session_duration_bucket",
        "event_sequence_number",
    ]
    return df[cleaned_columns], bad_records, report


def daily_user_summary(cleaned):
    purchases = cleaned.assign(purchase_amount=cleaned["purchase_value"].where(cleaned["event_type"] == "purchase", 0.0))
    return (
        purchases.groupby(["user_id", "date"], as_index=False)
        .agg(
            event_count=("event_id", "count"),
            session_count=("session_id", "nunique"),
            purchase_total=("purchase_amount", "sum"),
            first_seen=("event_ts", "min"),
            last_seen=("event_ts", "max"),
        )
    )


def hourly_event_volume(cleaned):
    return (
        cleaned.groupby(["event_type", "date", "hour"], as_index=False)
        .agg(
            event_count=("event_id", "count"),
            unique_users=("user_id", "nunique"),
            unique_sessions=("session_id", "nunique"),
        )
    )


def country_device_breakdown(cleaned):
    return (
        cleaned.groupby(["country_code", "device_type", "date"], as_index=False)
        .agg(
            event_count=("event_id", "count"),
            purchase_count=("event_type", lambda s: int((s == "purchase").sum())),
            unique_users=("user_id", "nunique"),
        )
    )


def user_reached_funnel(events):
    stage = 0
    targets = ["signup", "page_view", "click", "purchase"]
    reached = [False, False, False, False]
    for event_type in events:
        if stage < len(targets) and event_type == targets[stage]:
            reached[stage] = True
            stage += 1
    return pd.Series(
        {
            "signup_users": reached[0],
            "page_view_after_signup_users": reached[1],
            "click_after_page_view_users": reached[2],
            "purchase_after_click_users": reached[3],
        }
    )


def funnel_analysis(cleaned):
    ordered = cleaned.sort_values(["date", "user_id", "event_ts", "event_id"])
    per_user = ordered.groupby(["date", "user_id"])["event_type"].apply(list).reset_index()
    reached = per_user["event_type"].apply(user_reached_funnel)
    per_user = pd.concat([per_user[["date", "user_id"]], reached], axis=1)
    summary = (
        per_user.groupby("date", as_index=False)
        .agg(
            signup_users=("signup_users", "sum"),
            page_view_after_signup_users=("page_view_after_signup_users", "sum"),
            click_after_page_view_users=("click_after_page_view_users", "sum"),
            purchase_after_click_users=("purchase_after_click_users", "sum"),
        )
    )
    summary["signup_to_page_view_rate"] = safe_divide(
        summary["page_view_after_signup_users"], summary["signup_users"]
    )
    summary["page_view_to_click_rate"] = safe_divide(
        summary["click_after_page_view_users"], summary["page_view_after_signup_users"]
    )
    summary["click_to_purchase_rate"] = safe_divide(
        summary["purchase_after_click_users"], summary["click_after_page_view_users"]
    )
    summary["signup_to_purchase_rate"] = safe_divide(
        summary["purchase_after_click_users"], summary["signup_users"]
    )
    return summary


def safe_divide(numerator, denominator):
    return (numerator / denominator.where(denominator != 0)).fillna(0.0).round(6)


def reset_table_dir(output_dir, table_name):
    table_dir = Path(output_dir) / table_name
    if table_dir.exists():
        shutil.rmtree(table_dir)
    table_dir.mkdir(parents=True, exist_ok=True)
    return table_dir


def write_partitioned_parquet(df, output_dir, table_name):
    table_dir = reset_table_dir(output_dir, table_name)
    rows_written = 0
    if df.empty:
        return rows_written

    for date, partition in df.groupby("date", dropna=False):
        part_dir = table_dir / f"date={date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "part-000.parquet"
        write_df = partition.drop(columns=["date"])
        table = pa.Table.from_pandas(write_df, preserve_index=False)
        pq.write_table(table, file_path, compression="snappy", row_group_size=64_000)
        rows_written += len(partition)
    return int(rows_written)


def write_bad_records(bad_records, output_dir):
    table_dir = reset_table_dir(output_dir, "bad_records")
    if bad_records.empty:
        return 0

    bad = bad_records.copy()
    bad["date"] = "unknown"
    part_dir = table_dir / "date=unknown"
    part_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(bad.drop(columns=["date"]), preserve_index=False)
    pq.write_table(table, part_dir / "part-000.parquet", compression="snappy", row_group_size=64_000)
    return int(len(bad))


def write_manifest(manifest, manifest_path):
    path = Path(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def main():
    args = parse_args()
    configure_logging(args.log_level)
    started = time.perf_counter()
    run_timestamp = datetime.now(timezone.utc).isoformat()

    logging.info("pipeline run started")
    raw, input_files = read_jsonl_inputs(args.input_dir)
    logging.info("read %s input files with %s rows", len(input_files), len(raw))

    cleaned, bad_records, quality_report = clean_and_transform(raw)
    logging.warning("data quality report: %s", quality_report)

    output_rows = {}
    output_rows["cleaned_events"] = write_partitioned_parquet(cleaned, args.output_dir, "cleaned_events")
    output_rows["bad_records"] = write_bad_records(bad_records, args.output_dir)
    output_rows["daily_user_summary"] = write_partitioned_parquet(
        daily_user_summary(cleaned), args.output_dir, "daily_user_summary"
    )
    output_rows["hourly_event_volume"] = write_partitioned_parquet(
        hourly_event_volume(cleaned), args.output_dir, "hourly_event_volume"
    )
    output_rows["country_device_breakdown"] = write_partitioned_parquet(
        country_device_breakdown(cleaned), args.output_dir, "country_device_breakdown"
    )
    output_rows["funnel_analysis"] = write_partitioned_parquet(
        funnel_analysis(cleaned), args.output_dir, "funnel_analysis"
    )

    manifest = {
        "run_timestamp": run_timestamp,
        "status": "success",
        "input_file_count": len(input_files),
        "rows_ingested": int(len(raw)),
        "rows_dropped_per_qc_rule": quality_report,
        "rows_written_per_output_table": output_rows,
        "wall_clock_duration_seconds": round(time.perf_counter() - started, 3),
    }
    write_manifest(manifest, args.manifest_path)
    logging.info("pipeline run finished successfully")


if __name__ == "__main__":
    main()
