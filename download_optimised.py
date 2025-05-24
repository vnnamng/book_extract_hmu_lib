import concurrent.futures as cf
import threading
from io import BytesIO
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from PIL import Image
from reportlab.pdfgen import canvas
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ──────────────────────────────
# Thread-local session pool
# ──────────────────────────────
_thread_local = threading.local()


def _get_session(pool_maxsize: int):
    """Return a session stored on the *current* thread, creating it once."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90 Safari/537.36"
        )
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries, pool_maxsize=pool_maxsize)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _thread_local.session = s
    return _thread_local.session


# ──────────────────────────────
# Helpers
# ──────────────────────────────
def _make_base_img_url(reader_url: str, rel_path: str) -> str:
    parsed = urlparse(reader_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"
    if not rel_path.startswith("/"):
        rel_path = "/" + rel_path
    return urljoin(site_root, rel_path)


def _parse_reader_url(reader_url: str):
    q = parse_qs(urlparse(reader_url).query)
    total = int(q.get("TotalPage", [0])[0])
    ext = q.get("ext", ["jpg"])[0].lower()
    encoded = q.get("Url", [""])[0]
    if not encoded or total == 0:
        raise ValueError("Reader URL missing 'Url' or 'TotalPage'")
    rel = unquote(encoded).lstrip("/")
    if not rel.endswith("/"):
        rel += "/"
    return total, ext, _make_base_img_url(reader_url, rel)


# ──────────────────────────────
# Worker running in thread pool
# ──────────────────────────────
def _fetch_page(page: int, base_url: str, ext: str, pool_size: int):
    url = urljoin(base_url, f"{page:06d}.{ext}")
    s = _get_session(pool_size)
    resp = s.get(url, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Page {page}: HTTP {resp.status_code}")
    return page, resp.content


def _add_page_to_pdf(pdf_canvas, img_bytes: bytes):
    """Minimal Pillow work: open, ensure RGB, then embed."""
    with Image.open(BytesIO(img_bytes)) as img:
        if img.mode != "RGB":
            img = img.convert("RGB")
        w, h = img.size
        pdf_canvas.setPageSize((w, h))
        pdf_canvas.drawInlineImage(img, 0, 0, w, h)
        pdf_canvas.showPage()


# ──────────────────────────────
# Public API
# ──────────────────────────────
def download_and_stream_to_pdf_concurrent(
    reader_url: str,
    *,
    max_workers: int = 32,       # ← higher default for more oomph
) -> BytesIO:
    """High-speed, low-memory PDF builder for FullBookReader links."""
    total_pages, ext, base_url = _parse_reader_url(reader_url)

    pdf_buffer = BytesIO()
    pdf_canvas = canvas.Canvas(pdf_buffer, pageCompression=1)

    next_to_write = 1
    ready: dict[int, bytes] = {}

    # pool_maxsize ≥ workers × 2 avoids blocked connections
    pool_size = max_workers * 2

    with cf.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_page, p, base_url, ext, pool_size): p
            for p in range(1, total_pages + 1)
        }

        for fut in cf.as_completed(futures):
            page, data = fut.result()
            ready[page] = data

            while next_to_write in ready:
                _add_page_to_pdf(pdf_canvas, ready.pop(next_to_write))
                next_to_write += 1

    pdf_canvas.save()
    pdf_buffer.seek(0)
    return pdf_buffer
