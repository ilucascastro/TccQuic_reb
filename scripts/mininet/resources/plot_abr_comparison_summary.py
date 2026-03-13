#!/usr/bin/env python3

import csv
import math
import os
import sys

import matplotlib.pyplot as plt


PRIMARY_METRICS = [
    ("segment_completion_rate_fov_percent", "FOV completion (%)", False),
    ("deadline_miss_rate_fov_percent", "FOV deadline miss (%)", True),
    ("fov_hit_rate_delivery_percent", "FOV hit rate (%)", False),
    ("useful_goodput_fov_kbps", "Useful FOV goodput (kbps)", False),
]

ABR_ORDER = ["bola", "legacy"]
ABR_COLORS = {
    "bola": "tab:blue",
    "legacy": "tab:orange",
}


def read_rows(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def to_float(value, fallback=math.nan):
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def render(input_csv, output_png):
    rows = read_rows(input_csv)
    if not rows:
        raise ValueError("No rows found in consolidated CSV")

    grouped = {}
    for row in rows:
        label = f"S{row['scenario']} {row['server_scheduler']}"
        grouped.setdefault(label, {})
        grouped[label][row["abr_mode"]] = row

    labels = list(grouped.keys())
    x = list(range(len(labels)))
    width = 0.35

    fig, axes = plt.subplots(2, 2, figsize=(15, 8))
    axes = axes.flatten()

    for ax, (metric_key, title, lower_is_better) in zip(axes, PRIMARY_METRICS):
        for offset, abr in enumerate(ABR_ORDER):
            values = []
            for label in labels:
                row = grouped[label].get(abr)
                values.append(to_float(row.get(metric_key)) if row else math.nan)
            positions = [value + (offset - 0.5) * width for value in x]
            bars = ax.bar(positions, values, width=width, label=abr, color=ABR_COLORS[abr])
            for bar, value in zip(bars, values):
                if math.isnan(value):
                    continue
                text = f"{value:.1f}"
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    value + (1 if not lower_is_better or value >= 0 else 0.2),
                    text,
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )

        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=25, ha="right")
        ax.grid(axis="y", linestyle="--", alpha=0.3)
        if metric_key != "useful_goodput_fov_kbps":
            ax.set_ylim(0, 105)
        ax.legend()

    fig.suptitle("ABR Comparison Summary by Scenario and Scheduler", fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    os.makedirs(os.path.dirname(output_png) or ".", exist_ok=True)
    fig.savefig(output_png, dpi=150)
    plt.close(fig)


def main():
    if len(sys.argv) != 3:
        print("Usage: plot_abr_comparison_summary.py <INPUT.csv> <OUTPUT.png>")
        sys.exit(1)

    render(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
