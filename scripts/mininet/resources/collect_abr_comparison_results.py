#!/usr/bin/env python3

import csv
import os
import sys


OUTPUT_COLUMNS = [
    "scenario",
    "delay_ms",
    "background_load_pct",
    "server_scheduler",
    "abr_mode",
    "base_latency_ms",
    "join_latency_ms",
    "segment_completion_rate_percent",
    "segment_completion_rate_fov_percent",
    "stale_bytes_ratio_percent",
    "deadline_miss_rate_fov_percent",
    "deadline_miss_rate_nonfov_percent",
    "fov_hit_rate_delivery_percent",
    "useful_goodput_fov_kbps",
    "timely_bytes_ratio_percent",
]


def read_env_file(path):
    values = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    except FileNotFoundError:
        return {}
    return values


def read_first_row(path):
    try:
        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                return row
    except FileNotFoundError:
        return None
    return None


def find_summary_csv(directory):
    for name in os.listdir(directory):
        if name.startswith("statistics-summary-") and name.endswith(".csv"):
            return os.path.join(directory, name)
    return None


def collect_rows(root_dir):
    rows = []
    for current_root, _, files in os.walk(root_dir):
        if "experiment.env" not in files:
            continue
        summary_path = find_summary_csv(current_root)
        if summary_path is None:
            continue

        meta = read_env_file(os.path.join(current_root, "experiment.env"))
        summary = read_first_row(summary_path)
        if not summary:
            continue

        row = {
            "scenario": meta.get("scenario", ""),
            "delay_ms": meta.get("delay_ms", ""),
            "background_load_pct": meta.get("background_load_pct", ""),
            "server_scheduler": meta.get("server_mode", ""),
            "abr_mode": meta.get("abr_mode", ""),
            "base_latency_ms": meta.get("base_latency_ms", ""),
        }
        for key in OUTPUT_COLUMNS[6:]:
            row[key] = summary.get(key, "")
        rows.append(row)

    def sort_key(item):
        def as_int(value):
            try:
                return int(str(value).strip())
            except ValueError:
                return 10 ** 9

        return (
            as_int(item["scenario"]),
            item["server_scheduler"],
            item["abr_mode"],
            as_int(item["base_latency_ms"]),
        )

    rows.sort(key=sort_key)
    return rows


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    if len(sys.argv) != 3:
        print("Usage: collect_abr_comparison_results.py <LOG_ROOT> <OUTPUT.csv>")
        sys.exit(1)

    root_dir = sys.argv[1]
    output_path = sys.argv[2]
    rows = collect_rows(root_dir)
    write_csv(output_path, rows)
    print(f"[info] wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
