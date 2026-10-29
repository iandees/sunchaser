"""Stream a large HTTP(S) object to stdout using several concurrent range requests, in order.

A single connection to S3 often tops out well below the link speed; N ranges in flight multiply
it. Output is strictly sequential, so it can feed a pipe:

  python tools/fetch_parallel.py URL --threads 8 | osm-extract - out --history ...

Retries each range a few times; exits non-zero if a range keeps failing.
"""
import argparse
import concurrent.futures
import sys
import time
import urllib.request


def size_of(url):
    with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=60) as r:
        return int(r.headers["Content-Length"])


def fetch(url, start, end, attempts=6):
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
            with urllib.request.urlopen(req, timeout=120) as r:
                data = r.read()
            if len(data) == end - start + 1:
                return data
        except Exception as e:  # noqa: BLE001 - network errors of all kinds: retry
            print(f"range {start}-{end}: {e!r}, retry {i + 1}", file=sys.stderr, flush=True)
        time.sleep(2 ** i)
    raise SystemExit(f"range {start}-{end} failed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--chunk-mb", type=int, default=32)
    args = ap.parse_args()
    total = size_of(args.url)
    step = args.chunk_mb << 20
    ranges = [(s, min(s + step, total) - 1) for s in range(0, total, step)]
    out = sys.stdout.buffer
    t0, done = time.time(), 0
    with concurrent.futures.ThreadPoolExecutor(args.threads) as ex:
        window = args.threads * 2  # bounded read-ahead
        futures = {}
        for i, (a, b) in enumerate(ranges):
            if i >= window:
                break
            futures[i] = ex.submit(fetch, args.url, a, b)
        for i in range(len(ranges)):
            data = futures.pop(i).result()
            nxt = i + window
            if nxt < len(ranges):
                futures[nxt] = ex.submit(fetch, args.url, *ranges[nxt])
            out.write(data)
            done += len(data)
            if i % 64 == 0:
                rate = done / max(time.time() - t0, 1e-9) / 1e6
                print(f"fetch: {done / 1e9:.1f}/{total / 1e9:.1f} GB {rate:.0f} MB/s", file=sys.stderr, flush=True)
    out.flush()


if __name__ == "__main__":
    main()
