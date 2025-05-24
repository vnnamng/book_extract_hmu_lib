import concurrent.futures as cf
import math
import threading
from io import BytesIO
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from PIL import Image
from fpdf import FPDF
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ────────────────────────────────────────────────────────────────
# Thread-local connection pooling
# ────────────────────────────────────────────────────────────────
_thread_local = threading.local()


def _session(poolsize: int):
    if not hasattr(_thread_local, "s"):
        s = requests.Session()
        s.headers[
            "User-Agent"
        ] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/90"
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries, pool_maxsize=poolsize)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _thread_local.s = s
    return _thread_local.s


# ────────────────────────────────────────────────────────────────
# URL helpers
# ────────────────────────────────────────────────────────────────
def _make_base(reader_url: str, rel: str) -> str:
    parsed = urlparse(reader_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    if not rel.startswith("/"):
        rel = "/" + rel
    return urljoin(root, rel)


def _parse(reader_url: str):
    q = parse_qs(urlparse(reader_url).query)
    pages = int(q.get("TotalPage", [0])[0])
    ext = q.get("ext", ["jpg"])[0].lower()
    rel = unquote(q.get("Url", [""])[0]).lstrip("/")
    if not rel or pages == 0:
        raise ValueError("Reader URL missing 'Url' or 'TotalPage'")
    if not rel.endswith("/"):
        rel += "/"
    return pages, ext, _make_base(reader_url, rel)


# ────────────────────────────────────────────────────────────────
# Downloader
# ────────────────────────────────────────────────────────────────
def _fetch(page: int, base: str, ext: str, poolsize: int):
    url = urljoin(base, f"{page:06d}.{ext}")
    r = _session(poolsize).get(url, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Page {page}: HTTP {r.status_code}")
    return page, r.content


# ────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────
def download_and_build_pdf(
    reader_url: str,
    *,
    mem_budget_mb: int = 512,  # hard ceiling
    max_threads: int = 64,      # upper bound if RAM allows
) -> BytesIO:
    total, ext, base = _parse(reader_url)

    # ----- 1️⃣  Probe first page: decide thread count --------------------------
    probe_page, probe_bytes = _fetch(1, base, ext, poolsize=4)
    img_size = len(probe_bytes)

    # memory:   threads × img_size   + growing‐PDF   + fudge
    fudge = 32 * 1024 * 1024        # ~32 MB for Python objects, fonts, etc.
    budget = mem_budget_mb * 1024 * 1024
    threads = min(
        max_threads,
        max(2, (budget - fudge) // (img_size * 2)),  # ×2 ⇒ ½ RAM free for PDF
    )

    poolsize = threads * 2  # socket pool

    # container for out-of-order results
    ready: dict[int, bytes] = {probe_page: probe_bytes}
    next_write = 1

    # ----- 2️⃣  Set up PDF writer ---------------------------------------------
    pdf = FPDF(unit="pt")  # points → 1 pt ≈ 1 px

    def _add(page_bytes: bytes):
        with Image.open(BytesIO(page_bytes)) as img:
            w, h = img.size
            pdf.add_page(format=(w, h))
            # fpdf2 supports in-memory streams:
            pdf.image(img, x=0, y=0, w=w, h=h)

    # ----- 3️⃣  Kick off the remaining downloads ------------------------------
    with cf.ThreadPoolExecutor(max_workers=threads) as pool:
        fut_to_pg = {
            pool.submit(_fetch, p, base, ext, poolsize): p
            for p in range(2, total + 1)
        }

        #  Process downloads as soon as they arrive
        for fut in cf.as_completed(fut_to_pg):
            pg, data = fut.result()
            ready[pg] = data

            while next_write in ready:
                _add(ready.pop(next_write))
                next_write += 1

    # sanity: write any straggler (shouldn’t happen)
    for pg in sorted(ready):
        _add(ready[pg])

    # ----- 4️⃣  Return BytesIO -------------------------------------------------
    pdf_bytes = pdf.output(dest="S")
    buf = BytesIO(pdf_bytes)
    buf.seek(0)
    return buf

# download_and_build_pdf("https://thuvien.hmu.edu.vn/pages/cms/FullBookReader.aspx?Url=%2Fpages%2Fcms%2FTempDir%2Fbooks%2F202006150953-bc775b84-b0d0-49c1-b579-eddd928c30ee%2F%2FFullPreview&TotalPage=162&ext=jpg#page/4/mode/2up")
