from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
import boto3
import uvicorn
import os
import time
import threading
import logging
from collections import defaultdict
from botocore.config import Config
import urllib3  

# =========================
# SSL WARNING KAPAT
# =========================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# LOG NOISE REDUCTION
# =========================
logging.getLogger("botocore").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

app = FastAPI()

# =========================
# ENV
# =========================
_raw_endpoint = os.getenv("S3_ENDPOINT", "s3")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "xxx")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "xxx")
S3_BUCKET = os.getenv("S3_BUCKET", "bucket")
PORT = int(os.getenv("S3_PROXY_PORT", "30333"))

S3_USE_SSL = os.getenv("S3_USE_SSL", "false").lower() == "true"

RATE_LIMIT = int(os.getenv("RATE_LIMIT", "60"))     # per minute per IP
BURST = int(os.getenv("RATE_BURST", "500"))
RETRY_COUNT = int(os.getenv("S3_RETRY_COUNT", "3"))
TIMEOUT = int(os.getenv("S3_TIMEOUT", "30"))


# =========================
# ENDPOINT FIX
# =========================
if _raw_endpoint.startswith("http://") or _raw_endpoint.startswith("https://"):
    S3_ENDPOINT = _raw_endpoint
else:
    S3_ENDPOINT = ("https://" if S3_USE_SSL else "http://") + _raw_endpoint


# =========================
# S3 CLIENT (optimized)
# =========================
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    verify=False,
    config=Config(
        signature_version="s3v4",
        retries={"max_attempts": RETRY_COUNT},
        connect_timeout=TIMEOUT,
        read_timeout=TIMEOUT,
    ),
)


def format_headers(headers: dict) -> str:
    safe_headers = []

    for k, v in headers.items():
        # Ã§ uzun veya hassas header'larÃ½lt
        if k.lower() in ["authorization", "cookie"]:
            v = "***"

        safe_headers.append(f"{k}={v}")

    return ";".join(safe_headers)

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)


def log(ip, headers, req, status, start, s3_time):
    res_time = round(time.time() - start, 4)

    header_str = format_headers(headers)

    logging.info(
        f"{time.strftime('%Y-%m-%d %H:%M:%S')}|"
        f"{ip}|"
        f"{header_str}|"
        f"{req}|"
        f"{status}|"
        f"{res_time}|"
        f"{s3_time}"
    )

# =========================
# REAL CLIENT IP (XFF SAFE)
# =========================
def get_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host


# =========================
# TOKEN BUCKET RATE LIMIT
# =========================
class TokenBucket:
    def __init__(self, rate, burst):
        self.rate = rate / 60.0
        self.capacity = burst
        self.tokens = burst
        self.last = time.time()
        self.lock = threading.Lock()


buckets = defaultdict(lambda: TokenBucket(RATE_LIMIT, BURST))


def allow_request(ip: str) -> bool:
    bucket = buckets[ip]

    with bucket.lock:
        now = time.time()
        elapsed = now - bucket.last
        bucket.last = now

        bucket.tokens = min(bucket.capacity, bucket.tokens + elapsed * bucket.rate)

        if bucket.tokens >= 1:
            bucket.tokens -= 1
            return True

        return False


# =========================
# HEALTH CHECK (S3 CONNECTIVITY)
# =========================
@app.get("/health")
def health():
    try:
        s3.list_buckets()
        return {"status": "ok", "s3": "connected"}
    except Exception as e:
        return {"status": "fail", "error": str(e)}


# =========================
# PROXY
# =========================
@app.get("/{full_path:path}")
async def proxy(request: Request, full_path: str):

    start = time.time()
    ip = get_client_ip(request)

    # rate limit
    if not allow_request(ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    # /s3 cleanup
    if full_path.startswith("s3/"):
        full_path = full_path[3:]

    s3_start = time.time()

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=full_path)
        s3_time = round(time.time() - s3_start, 4)

        # streaming (video-safe)
        def stream():
            for chunk in iter(lambda: obj["Body"].read(1024 * 1024), b""):
                yield chunk

        log(ip, dict(request.headers), full_path, 200, start, s3_time)

        return StreamingResponse(
            stream(),
            media_type=obj.get("ContentType", "application/octet-stream"),
        )

    except Exception as e:
        s3_time = round(time.time() - s3_start, 4)
        log(ip, full_path, 404, start, s3_time)
        print("ERROR:", e)
        raise HTTPException(status_code=404, detail="Not found")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
