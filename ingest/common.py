"""Shared ingestion helpers: config, logging, SGT time, rate-limited GET with retry/backoff."""
import csv
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
SGT = timezone(timedelta(hours=8))


def load_config(path=None):
    with open(path or ROOT / "config" / "config.yaml") as f:
        return yaml.safe_load(f)


CFG = load_config()


def p(key):
    """Absolute local path for CFG['paths'][key]; parent directories are created."""
    path = ROOT / CFG["paths"][key]
    (path.parent if path.suffix else path).mkdir(parents=True, exist_ok=True)
    return path


def get_logger(name):
    d = ROOT / "outputs" / "logs"
    d.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(name)
    if not log.handlers:
        log.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        for h in (logging.FileHandler(d / f"{name}.log"), logging.StreamHandler(sys.stdout)):
            h.setFormatter(fmt)
            log.addHandler(h)
    return log


def now_sgt():
    return datetime.now(SGT).replace(microsecond=0)


def fmt_sgt(dt):
    """Format an SGT datetime the way the API expects (no offset)."""
    return dt.astimezone(SGT).strftime("%Y-%m-%dT%H:%M:%S")


def parse_sgt(s):
    """Parse '2026-10-15T12:26:13' (SGT, no offset) or an ISO string with offset."""
    dt = datetime.fromisoformat(s)
    return dt.replace(tzinfo=SGT) if dt.tzinfo is None else dt.astimezone(SGT)


class RateLimitedSession:
    """<= N requests per 10 s (sliding window), optional x-api-key header, retries on 429/5xx/timeouts,
    every call logged to outputs/logs/api_calls.csv."""

    def __init__(self, logger=None, max_per_10s=None):
        a = CFG["api"]
        self.max_n = max_per_10s or a["max_requests_per_10s"]
        self.retries, self.backoff, self.timeout = a["max_retries"], a["backoff_base"], a["timeout"]
        self.s = requests.Session()
        key = os.getenv("DATAGOV_API_KEY")
        if key:
            self.s.headers[a["api_key_header"]] = key
        self.calls = deque()
        self.lock = threading.Lock()
        self.log = logger
        self.n429 = 0
        self.call_log = ROOT / "outputs" / "logs" / "api_calls.csv"
        self.call_log.parent.mkdir(parents=True, exist_ok=True)

    def _wait_slot(self):
        with self.lock:
            while True:
                now = time.monotonic()
                while self.calls and now - self.calls[0] > 10.0:
                    self.calls.popleft()
                if len(self.calls) < self.max_n:
                    self.calls.append(now)
                    return
                time.sleep(10.0 - (now - self.calls[0]) + 0.05)

    def _record(self, url, status, ms, note=""):
        with self.lock, open(self.call_log, "a", newline="") as f:
            new = f.tell() == 0
            w = csv.writer(f)
            if new:
                w.writerow(["utc", "url", "status", "latency_ms", "note"])
            w.writerow([datetime.now(timezone.utc).isoformat(), url, status, ms, note])

    def get_json(self, url, params=None):
        last = None
        for attempt in range(1, self.retries + 1):
            self._wait_slot()
            t0 = time.monotonic()
            status = "ERR"
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
                status = r.status_code
                ms = int((time.monotonic() - t0) * 1000)
                self._record(r.url, status, ms)
                if status == 429:
                    with self.lock:
                        self.n429 += 1
                    raise RuntimeError("HTTP 429 rate limited")
                r.raise_for_status()
                return r.json()
            except Exception as e:  # noqa: BLE001 - retry anything, raise after max
                last = e
                if status == "ERR":
                    self._record(url, status, int((time.monotonic() - t0) * 1000), str(e)[:100])
                wait = self.backoff ** attempt
                if self.log:
                    self.log.warning("GET failed (%s, attempt %d/%d): %s; sleeping %.0fs", url, attempt, self.retries, e, wait)
                time.sleep(wait)
        raise RuntimeError(f"GET {url} failed after {self.retries} attempts: {last}")


def write_atomic(path, data, mode="w"):
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, mode) as f:
        f.write(data)
    os.replace(tmp, path)
