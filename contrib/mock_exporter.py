#!/usr/bin/env python3
"""
Mock Prometheus exporter for testing prom_the_reaper.

Serves 10 metric families, each with 10 time series (label combinations).
Values drift slightly on each scrape to simulate a real exporter.

Usage:
    python3 contrib/mock_exporter.py [--port 9100] [--host 127.0.0.1]

Then configure prom_the_reaper to scrape http://127.0.0.1:9100/metrics.
"""

import argparse
import math
import random
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

METRIC_FAMILIES = [
    ("http_requests_total",     "counter", "Total HTTP requests handled"),
    ("http_request_duration_seconds", "histogram", "HTTP request duration"),
    ("active_connections",      "gauge",   "Number of active connections"),
    ("memory_used_bytes",       "gauge",   "Memory currently in use"),
    ("cpu_seconds_total",       "counter", "Total CPU time consumed"),
    ("disk_read_bytes_total",   "counter", "Total bytes read from disk"),
    ("disk_write_bytes_total",  "counter", "Total bytes written to disk"),
    ("cache_hits_total",        "counter", "Total cache hits"),
    ("cache_misses_total",      "counter", "Total cache misses"),
    ("queue_depth",             "gauge",   "Current queue depth"),
]

INSTANCES = [f"instance{i:02d}" for i in range(10)]

# Persistent state so counters only go up
_counters: dict[str, float] = {}
_start_time = time.time()


def _counter(key: str, delta: float) -> float:
    _counters[key] = _counters.get(key, 0.0) + delta
    return _counters[key]


def generate_metrics() -> str:
    now = time.time()
    uptime = now - _start_time
    lines: list[str] = []

    for name, kind, help_text in METRIC_FAMILIES:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")

        if kind == "histogram":
            # Histogram: one per instance with _bucket/_sum/_count
            for inst in INSTANCES:
                base = 0.1 + 0.05 * math.sin(uptime / 30)
                count = int(_counter(f"{name}_{inst}_count", random.randint(1, 5)))
                total = _counter(f"{name}_{inst}_sum", base * random.uniform(0.8, 1.2))
                for le, frac in [("0.05", 0.5), ("0.1", 0.75), ("0.25", 0.9),
                                  ("0.5", 0.95), ("1.0", 0.99), ("+Inf", 1.0)]:
                    bucket = int(count * frac)
                    lines.append(f'{name}_bucket{{instance="{inst}",le="{le}"}} {bucket}')
                lines.append(f'{name}_sum{{instance="{inst}"}} {total:.4f}')
                lines.append(f'{name}_count{{instance="{inst}"}} {count}')
        elif kind == "counter":
            for inst in INSTANCES:
                val = _counter(f"{name}_{inst}", random.uniform(1, 50))
                lines.append(f'{name}{{instance="{inst}"}} {val:.2f}')
        else:  # gauge
            for inst in INSTANCES:
                seed = hash((name, inst)) % 1000
                val = seed + 50 * math.sin(uptime / 20 + seed)
                lines.append(f'{name}{{instance="{inst}"}} {val:.2f}')

    lines.append("")
    return "\n".join(lines)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        body = generate_metrics().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"[mock_exporter] {self.address_string()} - {fmt % args}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9100)
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), Handler)
    print(f"Mock exporter listening on http://{args.host}:{args.port}/metrics")
    print(f"Serving {len(METRIC_FAMILIES)} metric families × {len(INSTANCES)} instances each")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
