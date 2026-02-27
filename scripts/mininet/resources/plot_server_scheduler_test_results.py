#!/usr/bin/env python3

import csv
import os
import sys
import datetime as dt
import matplotlib.pyplot as plt


CLASS_NAMES = {0: 'high', 1: 'medium', 2: 'low'}
CLASS_ORDER = ['high', 'medium', 'low']
CLASS_COLORS = {
    'high': 'tab:red',
    'medium': 'tab:orange',
    'low': 'tab:blue',
}


def _bool(v: str) -> bool:
    return str(v).strip().lower() in ('1', 'true', 'yes')


def _read_csv(path: str):
    try:
        with open(path, newline='') as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return None


def _rel_seconds_from_iso(ts_list):
    times = []
    for ts in ts_list:
        try:
            times.append(dt.datetime.fromisoformat(ts))
        except Exception:
            pass
    if not times:
        return [], None
    t0 = min(times)
    return [(t - t0).total_seconds() for t in times], t0


def _rel_seconds_from_ns(ns_list):
    try:
        ns_list = [int(n) for n in ns_list]
    except Exception:
        return [], None
    if not ns_list:
        return [], None
    t0 = min(ns_list)
    return [((n - t0) / 1e9) for n in ns_list], t0


# -------------------- Client plot --------------------
def render_client_plot(input_files, output_dir):
    # Prefer aggregated client summary if present
    summary_files = [p for p in input_files if os.path.basename(p).startswith('statistics-summary-')]
    metrics = {}
    if summary_files:
        rows = _read_csv(summary_files[0]) or []
        if rows:
            row = rows[0]
            # Only the three simple metrics
            if 'segment_completion_rate_percent' in row:
                try:
                    metrics['segment_completion_rate_percent'] = float(row['segment_completion_rate_percent'])
                except Exception:
                    pass
            if 'segment_completion_rate_fov_percent' in row:
                try:
                    metrics['segment_completion_rate_fov_percent'] = float(row['segment_completion_rate_fov_percent'])
                except Exception:
                    pass
            if 'stale_bytes_ratio_percent' in row:
                try:
                    metrics['stale_bytes_ratio_percent'] = float(row['stale_bytes_ratio_percent'])
                except Exception:
                    pass
            if 'timely_bytes_ratio_percent' in row:
                try:
                    metrics['timely_bytes_ratio_percent'] = float(row['timely_bytes_ratio_percent'])
                except Exception:
                    pass
    else:
        # Fallback: compute completion rates from statistics-*.csv
        stats_files = [p for p in input_files if os.path.basename(p).startswith('statistics-') and 'summary' not in os.path.basename(p)]
        ok_total = 0
        all_total = 0
        ok_fov = 0
        fov_total = 0
        for path in stats_files:
            rows = _read_csv(path) or []
            for r in rows:
                all_total += 1
                if _bool(r.get('ok', 'false')):
                    ok_total += 1
                if _bool(r.get('in_fov', 'false')):
                    fov_total += 1
                    if _bool(r.get('ok', 'false')):
                        ok_fov += 1
        if all_total > 0:
            metrics['segment_completion_rate_percent'] = 100.0 * ok_total / all_total
        if fov_total > 0:
            metrics['segment_completion_rate_fov_percent'] = 100.0 * ok_fov / fov_total
        # stale_bytes_ratio_percent not available from client fallback

    # If nothing to plot, return silently
    if not metrics:
        print('[warn] No client metrics found to plot')
        return

    labels = []
    values = []
    for key in (
        'segment_completion_rate_percent',
        'segment_completion_rate_fov_percent',
        'stale_bytes_ratio_percent',
        'timely_bytes_ratio_percent',
    ):
        if key in metrics:
            labels.append(key.replace('_', '\n'))
            values.append(metrics[key])

    plt.figure(figsize=(6, 4))
    bars = plt.bar(range(len(values)), values, color='tab:blue')
    plt.ylim(0, 100)
    plt.xticks(range(len(labels)), labels)
    plt.ylabel('percent')
    plt.title('Client Summary')
    for b, v in zip(bars, values):
        plt.text(b.get_x() + b.get_width() / 2, v + 1, f'{v:.1f}%', ha='center', va='bottom', fontsize=8)
    os.makedirs(output_dir, exist_ok=True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'client.png'))
    plt.close()


# -------------------- Server plot --------------------
def render_server_plot(input_files, output_dir):
    # Prefer queue_len.csv (lines per class over time)
    ql_files = [p for p in input_files if os.path.basename(p) == 'queue_len.csv']
    if ql_files:
        rows = _read_csv(ql_files[0]) or []
        if rows:
            by_class = {c: {'ts': [], 'len': []} for c in CLASS_ORDER}
            for r in rows:
                c = r.get('class')
                if c in by_class:
                    by_class[c]['ts'].append(r.get('ts'))
                    try:
                        by_class[c]['len'].append(int(float(r.get('queue_len', '0'))))
                    except Exception:
                        by_class[c]['len'].append(0)

            # Compute shared relative time
            all_ts = []
            for c in CLASS_ORDER:
                all_ts.extend(by_class[c]['ts'])
            rel_all, t0 = _rel_seconds_from_iso(all_ts)
            if t0 is None:
                print('[warn] Could not parse queue_len timestamps')
                return
            ts_map = {iso: rel for iso, rel in zip(all_ts, rel_all)}

            plt.figure(figsize=(7, 4))
            drew = False
            for c in CLASS_ORDER:
                if by_class[c]['ts']:
                    x = [ts_map[ts] for ts in by_class[c]['ts'] if ts in ts_map]
                    y = by_class[c]['len']
                    if x and y:
                        plt.plot(x, y, label=c, color=CLASS_COLORS.get(c))
                        drew = True
            if not drew:
                plt.close()
                print('[warn] No queue_len series to draw')
                return
            plt.xlabel('time (s)')
            plt.ylabel('queue length (pkts)')
            plt.legend()
            os.makedirs(output_dir, exist_ok=True)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'server.png'))
            plt.close()
            return

    # Fallback: reqlog.csv scatter of response time by class
    rl_files = [p for p in input_files if os.path.basename(p) == 'reqlog.csv']
    if rl_files:
        rows = _read_csv(rl_files[0]) or []
        if rows:
            ns_times = []
            classes = []
            rsp = []
            for r in rows:
                try:
                    ns_times.append(int(r['time_ns']))
                    classes.append(CLASS_NAMES.get(int(r['class']), str(r['class'])))
                    rsp.append(float(r['rsp_ms']))
                except Exception:
                    pass
            rel_t, _ = _rel_seconds_from_ns(ns_times)
            per_class_x = {c: [] for c in CLASS_ORDER}
            per_class_y = {c: [] for c in CLASS_ORDER}
            for t, c, v in zip(rel_t, classes, rsp):
                if c in per_class_x:
                    per_class_x[c].append(t)
                    per_class_y[c].append(v)
            plt.figure(figsize=(7, 4))
            drew = False
            for c in CLASS_ORDER:
                x, y = per_class_x[c], per_class_y[c]
                if x and y:
                    plt.scatter(x, y, s=2, label=c, color=CLASS_COLORS.get(c))
                    drew = True
            if not drew:
                plt.close()
                print('[warn] No reqlog points to draw')
                return
            plt.xlabel('time (s)')
            plt.ylabel('response time (ms)')
            plt.legend()
            os.makedirs(output_dir, exist_ok=True)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'server.png'))
            plt.close()
            return

    print('[warn] No server inputs found (queue_len.csv or reqlog.csv)')


def main():
    if len(sys.argv) < 2:
        print('Usage A (list of CSVs): plot_server_scheduler_test_results.py <INPUT1.csv> [INPUT2.csv ...] <OUTPUT_DIR>')
        print('Usage B (dirs):         plot_server_scheduler_test_results.py <INPUT_DIR> <OUTPUT_DIR>')
        sys.exit(1)

    args = sys.argv[1:]
    # If last arg is a directory, treat it as output
    if os.path.isdir(args[-1]):
        output_dir = args[-1]
        inputs = args[:-1]
        # If inputs are empty but first is a directory, use its CSVs
        input_files = []
        if len(inputs) == 1 and os.path.isdir(inputs[0]):
            in_dir = inputs[0]
            try:
                for name in os.listdir(in_dir):
                    if name.lower().endswith('.csv'):
                        input_files.append(os.path.join(in_dir, name))
            except Exception:
                pass
        else:
            # Filter only existing CSV files
            for p in inputs:
                if os.path.isfile(p) and p.lower().endswith('.csv'):
                    input_files.append(p)
    else:
        # If last arg is not a directory, assume the classic mode <CSV> <OUTPUT_DIR>
        if len(args) != 2:
            print('error: expected <CSV> <OUTPUT_DIR> or <CSV...> <OUTPUT_DIR>')
            sys.exit(1)
        input_files = [args[0]] if os.path.isfile(args[0]) else []
        output_dir = args[1]

    # De-duplicate keeping order
    seen = set()
    ordered_inputs = []
    for p in input_files:
        if p not in seen:
            ordered_inputs.append(p)
            seen.add(p)

    # Render the two simple plots
    print('Rendering client plot...')
    render_client_plot(ordered_inputs, output_dir)
    print('Rendering server plot...')
    render_server_plot(ordered_inputs, output_dir)


if __name__ == '__main__':
    main()
