"""A DuckLake catalog kept on object storage: no always-on database.

The authoritative catalog is a DuckDB-format DuckLake file stored next to the data, e.g.
s3://bucket/lake/catalog.ducklake. Readers ATTACH it directly (read-only, over httpfs) and reopen
it when its ETag changes. The single writer is a scheduled job (updater.py job): it downloads the
catalog, applies whatever is pending, and uploads it with a conditional PUT on the ETag it
downloaded (If-Match; If-None-Match: * for the first upload). If another job got there first, the
PUT fails with 412 and the work is discarded: the catalog never forks, and Parquet files the loser
wrote are unreferenced orphans that compaction removes. Between jobs nothing runs.

AWS S3 and Cloudflare R2 both support conditional PUTs. Credentials: lakeconn's SUNCHASER_S3_*.

  python src/objcatalog.py put local.ducklake s3://bucket/lake/catalog.ducklake   # first upload
  python src/objcatalog.py get s3://bucket/lake/catalog.ducklake local.ducklake
"""
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import lakeconn  # noqa: E402


class Conflict(Exception):
    """The catalog changed since it was downloaded (another writer committed)."""


def _client():
    import boto3
    from botocore.config import Config
    env = lakeconn.env
    endpoint = env("S3_ENDPOINT")
    if endpoint and not endpoint.startswith("http"):
        endpoint = ("https://" if env("S3_USE_SSL", "true") == "true" else "http://") + endpoint
    style = "path" if env("S3_URL_STYLE", "path" if endpoint else "virtual") == "path" else "virtual"
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=env("S3_KEY_ID") or None,
                        aws_secret_access_key=env("S3_SECRET") or None,
                        region_name=env("S3_REGION") or None, config=Config(s3={"addressing_style": style}))


def _split(url):
    if not url.startswith("s3://"):
        raise ValueError(f"not an s3:// URL: {url}")
    bucket, _, key = url[5:].partition("/")
    return bucket, key


def head(url):
    """ETag of the catalog, or None if it doesn't exist."""
    from botocore.exceptions import ClientError
    bucket, key = _split(url)
    try:
        return _client().head_object(Bucket=bucket, Key=key)["ETag"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def fetch(url, local):
    """Download the catalog; returns its ETag (the token for the conditional upload)."""
    bucket, key = _split(url)
    r = _client().get_object(Bucket=bucket, Key=key)
    tmp = f"{local}.part"
    with open(tmp, "wb") as f:
        for chunk in iter(lambda: r["Body"].read(1 << 20), b""):
            f.write(chunk)
    os.replace(tmp, local)
    return r["ETag"]


def push(local, url, etag):
    """Upload the catalog if it is still the version `etag` (None: only if it doesn't exist yet).
    Returns the new ETag; raises Conflict if another writer committed in between."""
    from botocore.exceptions import ClientError
    bucket, key = _split(url)
    cond = {"IfMatch": etag} if etag else {"IfNoneMatch": "*"}
    with open(local, "rb") as f:
        try:
            return _client().put_object(Bucket=bucket, Key=key, Body=f, **cond)["ETag"]
        except ClientError as e:
            if e.response["Error"]["Code"] in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
                raise Conflict(f"{url} changed since it was read (another writer committed)") from e
            raise


def acquire_lease(url, ttl):
    """A writer lease next to the catalog (<catalog>.lease), so overlapping scheduled runs don't
    both do the work: the second one sees the lease and exits. Created with If-None-Match: *; an
    expired lease (a crashed run) is taken over with If-Match on its ETag. Returns the lease's ETag,
    or None if another run holds it. The conditional catalog PUT remains the actual guarantee."""
    import json
    import time
    from botocore.exceptions import ClientError
    bucket, key = _split(url + ".lease")
    s3 = _client()
    body = json.dumps({"expires": time.time() + ttl}).encode()
    try:
        return s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")["ETag"]
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
            raise
    try:
        cur = s3.get_object(Bucket=bucket, Key=key)
    except ClientError:
        return None  # released meanwhile: let the next run take it
    if json.loads(cur["Body"].read()).get("expires", 0) > time.time():
        return None
    try:
        return s3.put_object(Bucket=bucket, Key=key, Body=body, IfMatch=cur["ETag"])["ETag"]
    except ClientError:
        return None


def renew_lease(url, etag, ttl):
    """Extend a lease this writer holds (If-Match on its ETag). Returns the new ETag, or None if
    the lease was taken over (it had expired)."""
    import json
    import time
    from botocore.exceptions import ClientError
    bucket, key = _split(url + ".lease")
    body = json.dumps({"expires": time.time() + ttl}).encode()
    try:
        return _client().put_object(Bucket=bucket, Key=key, Body=body, IfMatch=etag)["ETag"]
    except ClientError:
        return None


def release_lease(url):
    bucket, key = _split(url + ".lease")
    _client().delete_object(Bucket=bucket, Key=key)


def copy(url, dest_url):
    """Server-side copy, e.g. to keep a versioned history of catalogs for rollback."""
    bucket, key = _split(url)
    dbucket, dkey = _split(dest_url)
    _client().copy_object(Bucket=dbucket, Key=dkey, CopySource={"Bucket": bucket, "Key": key})


def prune(prefix_url, keep):
    """Delete all but the newest `keep` objects under a prefix (catalog copies are named by
    timestamp, so name order is time order). Returns the number deleted."""
    bucket, prefix = _split(prefix_url.rstrip("/") + "/")
    s3 = _client()
    keys = sorted(o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
                  for o in page.get("Contents", []))
    old = keys[:max(len(keys) - keep, 0)]
    for i in range(0, len(old), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in old[i:i + 1000]], "Quiet": True})
    return len(old)


def main():
    if len(sys.argv) != 4 or sys.argv[1] not in ("get", "put"):
        sys.exit(__doc__)
    if sys.argv[1] == "get":
        print(fetch(sys.argv[2], sys.argv[3]))
    else:
        print(push(sys.argv[2], sys.argv[3], head(sys.argv[3])))


if __name__ == "__main__":
    main()
