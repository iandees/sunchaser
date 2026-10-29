"""Tiny object-store stand-in: serves a directory over HTTP with Range support.

Adds a fixed first-byte latency per request (to mimic S3 GETs) and counts requests/bytes.
  GET /__stats  -> {"requests": n, "bytes": b, "by_file": {...}}
  GET /__reset  -> zero the counters
  GET /__latency?ms=40 -> change the per-request latency
  GET /<bucket>?list-type=2&prefix=..&delimiter=..  -> minimal S3 ListObjectsV2 (path-style), so
      S3 clients (Lance's object_store, DuckDB s3://) can use the server as an endpoint.
"""
import argparse
import json
import os
import socket
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlparse
from xml.sax.saxutils import escape

LOCK = threading.Lock()
STATS = {"requests": 0, "bytes": 0, "by_file": {}}
CONFIG = {"latency_ms": 0.0}


def record(path, n):
    with LOCK:
        STATS["requests"] += 1
        STATS["bytes"] += n
        f = STATS["by_file"].setdefault(path, [0, 0])
        f[0] += 1
        f[1] += n


class Handler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    wbufsize = 1 << 16  # send headers + body together

    def setup(self):
        super().setup()
        # Without this, header/body writes on keep-alive sockets hit Nagle + delayed ACK (~40 ms each).
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def log_message(self, *a):
        pass

    def _control(self):
        u = urlparse(self.path)
        if u.path == "/__stats":
            with LOCK:
                body = json.dumps(STATS).encode()
        elif u.path == "/__reset":
            with LOCK:
                STATS.update(requests=0, bytes=0, by_file={})
            body = b"{}"
        elif u.path == "/__latency":
            CONFIG["latency_ms"] = float(parse_qs(u.query)["ms"][0])
            body = json.dumps(CONFIG).encode()
        elif "list-type=2" in (u.query or ""):
            body = self._list_objects(u)
            record(u.path + "?list", len(body))
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return True
        else:
            return False
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        return True

    def _list_objects(self, u):
        if CONFIG["latency_ms"]:
            time.sleep(CONFIG["latency_ms"] / 1000)
        q = {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
        bucket = unquote(u.path).strip("/")
        prefix, delim = q.get("prefix", ""), q.get("delimiter", "")
        root = os.path.join(os.getcwd(), bucket)
        keys, prefixes = [], set()
        for dirpath, _, files in os.walk(root):
            for f in files:
                key = os.path.relpath(os.path.join(dirpath, f), root).replace(os.sep, "/")
                if not key.startswith(prefix):
                    continue
                rest = key[len(prefix):]
                if delim and delim in rest:
                    prefixes.add(prefix + rest.split(delim)[0] + delim)
                else:
                    keys.append(key)
        items = "".join(
            f"<Contents><Key>{escape(k)}</Key><Size>{os.path.getsize(os.path.join(root, k))}</Size>"
            f"<LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>\"{abs(hash(k))}\"</ETag></Contents>"
            for k in sorted(keys))
        cps = "".join(f"<CommonPrefixes><Prefix>{escape(p)}</Prefix></CommonPrefixes>" for p in sorted(prefixes))
        return (f'<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>{escape(bucket)}</Name>'
                f"<Prefix>{escape(prefix)}</Prefix><KeyCount>{len(keys) + len(prefixes)}</KeyCount>"
                f"<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{items}{cps}</ListBucketResult>").encode()

    def _file(self):
        path = self.translate_path(unquote(urlparse(self.path).path))
        if not os.path.isfile(path):
            self.send_error(404)
            return None, None
        return path, os.path.getsize(path)

    def do_HEAD(self):
        path, size = self._file()
        if path is None:
            return
        if CONFIG["latency_ms"]:
            time.sleep(CONFIG["latency_ms"] / 1000)
        record(urlparse(self.path).path, 0)
        self.send_response(200)
        self.send_header("Content-Length", str(size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Last-Modified", self.date_time_string(int(os.path.getmtime(path))))
        self.end_headers()

    def do_GET(self):
        if self._control():
            return
        path, size = self._file()
        if path is None:
            return
        start, end = 0, size - 1
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            a, b = rng[6:].split("-")
            if a == "":
                start = max(0, size - int(b))
            else:
                start = int(a)
                end = min(size - 1, int(b)) if b else size - 1
        if CONFIG["latency_ms"]:
            time.sleep(CONFIG["latency_ms"] / 1000)
        n = end - start + 1
        record(urlparse(self.path).path, n)
        self.send_response(206 if rng else 200)
        self.send_header("Content-Length", str(n))
        self.send_header("Accept-Ranges", "bytes")
        if rng:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Last-Modified", self.date_time_string(int(os.path.getmtime(path))))
        self.end_headers()
        with open(path, "rb") as f:
            f.seek(start)
            remaining = n
            while remaining:
                chunk = f.read(min(1 << 20, remaining))
                self.wfile.write(chunk)
                remaining -= len(chunk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--latency-ms", type=float, default=0)
    args = ap.parse_args()
    CONFIG["latency_ms"] = args.latency_ms
    os.chdir(args.root)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer.request_queue_size = 1024  # default 5: concurrent S3 clients (Lance) get refused
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
