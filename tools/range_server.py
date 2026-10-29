"""Tiny object-store stand-in: serves a directory over HTTP with Range support.

Adds a fixed first-byte latency per request (to mimic S3 GETs) and counts requests/bytes.
  GET /__stats  -> {"requests": n, "bytes": b, "by_file": {...}}
  GET /__reset  -> zero the counters
  GET /__latency?ms=40 -> change the per-request latency
  GET /<bucket>?list-type=2&prefix=..&delimiter=..  -> minimal S3 ListObjectsV2 (path-style), so
      S3 clients (Lance's object_store, DuckDB s3://) can use the server as an endpoint.
  PUT /<bucket>/<key> (If-None-Match: * honoured), DELETE, and multipart uploads
      (POST ?uploads, PUT ?partNumber&uploadId, POST ?uploadId, DELETE ?uploadId): enough S3 for
      DuckDB/DuckLake and Lance writers. Requests are not authenticated.
"""
import argparse
import json
import os
import hashlib
import re
import shutil
import socket
import threading
import uuid
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlparse
from xml.sax.saxutils import escape

LOCK = threading.Lock()
STATS = {"requests": 0, "bytes": 0, "by_file": {}}
PUT_LOCK = threading.Lock()


def file_etag(path):
    """Changes whenever the object is replaced (S3 ETags are content hashes; any unique value works)."""
    st = os.stat(path)
    return f'"{st.st_mtime_ns:x}-{st.st_size:x}-{st.st_ino:x}"'


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

    # ---- writes ------------------------------------------------------------------

    def _local(self):
        rel = unquote(urlparse(self.path).path).lstrip("/")
        path = os.path.normpath(os.path.join(os.getcwd(), rel))
        if not path.startswith(os.getcwd() + os.sep):
            raise PermissionError(rel)
        return path

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _reply(self, code, body=b"", ctype="application/xml", headers=()):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _uploads(self, upload_id):
        return os.path.join(os.getcwd(), ".uploads", re.sub(r"[^A-Za-z0-9-]", "", upload_id))

    def do_PUT(self):
        if CONFIG["latency_ms"]:
            time.sleep(CONFIG["latency_ms"] / 1000)
        q = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
        data = self._body()
        record(urlparse(self.path).path + "?put", len(data))
        etag = '"' + hashlib.md5(data).hexdigest() + '"'
        if "uploadId" in q:
            d = self._uploads(q["uploadId"])
            with open(os.path.join(d, f"{int(q['partNumber']):05d}"), "wb") as f:
                f.write(data)
            return self._reply(200, headers=[("ETag", etag)])
        path = self._local()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.{uuid.uuid4().hex}.tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        with PUT_LOCK:  # conditional writes: check and replace atomically, like S3
            if self.headers.get("If-None-Match") == "*" and os.path.exists(path):
                os.remove(tmp)
                return self._reply(412, b"<Error><Code>PreconditionFailed</Code></Error>")
            want = self.headers.get("If-Match")
            if want is not None and (not os.path.exists(path) or file_etag(path) != want):
                os.remove(tmp)
                return self._reply(412, b"<Error><Code>PreconditionFailed</Code></Error>")
            os.replace(tmp, path)  # atomic, like an S3 object PUT
            etag = file_etag(path)
        self._reply(200, headers=[("ETag", etag)])

    def do_POST(self):
        if CONFIG["latency_ms"]:
            time.sleep(CONFIG["latency_ms"] / 1000)
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
        body = self._body()
        record(u.path + "?post", len(body))
        key = unquote(u.path).lstrip("/")
        if "delete" in q:  # DeleteObjects
            bucket = key.split("/")[0]
            for k in re.findall(rb"<Key>([^<]+)</Key>", body):
                try:
                    os.remove(os.path.normpath(os.path.join(os.getcwd(), bucket, k.decode())))
                except FileNotFoundError:
                    pass
            return self._reply(200, b'<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>')
        if "uploads" in q:
            upload_id = uuid.uuid4().hex
            os.makedirs(self._uploads(upload_id), exist_ok=True)
            bucket, _, k = key.partition("/")
            xml = (f'<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>{escape(bucket)}'
                   f"</Bucket><Key>{escape(k)}</Key><UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>")
            return self._reply(200, xml.encode())
        if "uploadId" in q:
            d = self._uploads(q["uploadId"])
            path = self._local()
            os.makedirs(os.path.dirname(path), exist_ok=True)
            parts = [int(n) for n in re.findall(rb"<PartNumber>(\d+)</PartNumber>", body)] or \
                sorted(int(n) for n in os.listdir(d))
            tmp = f"{path}.{uuid.uuid4().hex}.tmp"
            with open(tmp, "wb") as out:
                for n in parts:
                    with open(os.path.join(d, f"{n:05d}"), "rb") as f:
                        shutil.copyfileobj(f, out)
            os.replace(tmp, path)
            shutil.rmtree(d, ignore_errors=True)
            bucket, _, k = key.partition("/")
            xml = (f'<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>{escape(bucket)}'
                   f'</Bucket><Key>{escape(k)}</Key><ETag>"{uuid.uuid4().hex}"</ETag></CompleteMultipartUploadResult>')
            return self._reply(200, xml.encode())
        self._reply(400, b"<Error><Code>InvalidRequest</Code></Error>")

    def do_DELETE(self):
        q = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
        record(urlparse(self.path).path + "?delete", 0)
        if "uploadId" in q:
            shutil.rmtree(self._uploads(q["uploadId"]), ignore_errors=True)
        else:
            try:
                os.remove(self._local())
            except FileNotFoundError:
                pass
        self._reply(204)

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
        self.send_header("ETag", file_etag(path))
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
        self.send_header("ETag", file_etag(path))
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
