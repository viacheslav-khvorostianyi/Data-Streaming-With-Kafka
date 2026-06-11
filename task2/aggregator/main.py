import json
import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def _compute_metrics(exp_dir: Path) -> dict | None:
    meta_path = exp_dir / "meta.json"
    if not meta_path.exists():
        log.warning("No meta.json in %s — skipping", exp_dir)
        return None

    meta = json.loads(meta_path.read_text())

    csv_files = [f for f in exp_dir.glob("*.csv") if f.stat().st_size > 0]
    if not csv_files:
        log.warning("No CSVs in %s — skipping", exp_dir)
        return None

    frames = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            log.warning("Could not read %s: %s", f, exc)

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)

    total_bytes = df["payload_bytes"].sum()
    first_send = df["send_timestamp"].min()
    last_finish = df["finish_timestamp"].max()
    duration = last_finish - first_send

    if duration <= 0:
        log.warning("Duration is zero in %s — skipping", exp_dir)
        return None

    throughput_mbps = total_bytes / duration / 1_000_000
    max_latency_s = (df["finish_timestamp"] - df["send_timestamp"]).max()

    return {
        **meta,
        "total_messages": len(df),
        "total_bytes": int(total_bytes),
        "duration_s": round(duration, 2),
        "throughput_mbps": round(throughput_mbps, 6),
        "max_latency_s": round(max_latency_s, 2),
    }


def _short_label(row: dict) -> str:
    return f"{row['num_producers']}p/{row['num_partitions']}part/{row['num_consumers']}c"


def main() -> None:
    log_dir = Path(config.LOG_DIR)
    report_dir = Path(config.REPORT_DIR)
    report_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for exp_dir in sorted(log_dir.iterdir()):
        if not exp_dir.is_dir():
            continue
        metrics = _compute_metrics(exp_dir)
        if metrics:
            results.append(metrics)
            log.info(
                "%-38s  throughput=%8.4f Mbps  max_latency=%6.2f s  msgs=%d",
                metrics["exp_label"],
                metrics["throughput_mbps"],
                metrics["max_latency_s"],
                metrics["total_messages"],
            )

    if not results:
        log.error("No valid experiment results found in %s", log_dir)
        return

    summary = pd.DataFrame(results)
    summary_path = report_dir / "summary.csv"
    summary.to_csv(summary_path, index=False)
    log.info("Summary saved → %s", summary_path)

    short_labels = [_short_label(r) for r in results]

    # --- Throughput chart ---
    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.bar(range(len(results)), summary["throughput_mbps"], color="steelblue")
    ax.set_xticks(range(len(results)))
    ax.set_xticklabels(short_labels, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Throughput (Mbps)")
    ax.set_title("System Throughput per Configuration")
    ax.bar_label(bars, fmt="%.4f", padding=3, fontsize=8)
    plt.tight_layout()
    throughput_path = report_dir / "throughput.png"
    plt.savefig(throughput_path, dpi=150)
    plt.close()
    log.info("Throughput chart → %s", throughput_path)

    # --- Latency chart ---
    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.bar(range(len(results)), summary["max_latency_s"], color="coral")
    ax.set_xticks(range(len(results)))
    ax.set_xticklabels(short_labels, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Max Latency (s)")
    ax.set_title("Max End-to-End Latency per Configuration")
    ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=8)
    plt.tight_layout()
    latency_path = report_dir / "latency.png"
    plt.savefig(latency_path, dpi=150)
    plt.close()
    log.info("Latency chart → %s", latency_path)

    # --- Summary table ---
    print("\n" + "=" * 80)
    print("EXPERIMENT SUMMARY")
    print("=" * 80)
    display_cols = [
        "exp_label", "num_producers", "num_partitions", "num_consumers",
        "total_messages", "throughput_mbps", "max_latency_s",
    ]
    print(summary[display_cols].to_string(index=False))
    print("=" * 80)


if __name__ == "__main__":
    main()
