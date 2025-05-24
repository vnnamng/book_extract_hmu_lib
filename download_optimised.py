import concurrent.futures as cf
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from PIL import Image
from reportlab.pdfgen import canvas
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _make_base_img_url(reader_url: str, rel_path: str) -> str:
    parsed = urlparse(reader_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"
    if not rel_path.startswith("/"):
        rel_path = "/" + rel_path
    return urljoin(site_root, rel_path)


def _create_session() -> requests.Session:
    """One session per worker thread, with retries."""
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
    adapter = HTTPAdapter(max_retries=retries, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _parse_reader_url(reader_url: str):
    q = parse_qs(urlparse(reader_url).query)
    pages = int(q.get("TotalPage", [0])[0])
    ext = q.get("ext", ["jpg"])[0].lower()
    encoded_path = q.get("Url", [""])[0]
    if not encoded_path or pages == 0:
        raise ValueError("Reader URL missing 'Url' or 'TotalPage'")
    rel_path = unquote(encoded_path).lstrip("/")
    if not rel_path.endswith("/"):
        rel_path += "/"
    base_url = _make_base_img_url(reader_url, rel_path)
    return pages, ext, base_url


# ──────────────────────────────────────────────────────────────────────────────
# Worker (runs in thread pool)
# ──────────────────────────────────────────────────────────────────────────────
def _fetch_page(page: int, base_url: str, ext: str) -> tuple[int, bytes]:
    """Download one image and return (page_number, image_bytes)."""
    url = urljoin(base_url, f"{page:06d}.{ext}")
    with _create_session() as s:
        resp = s.get(url, timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Page {page}: HTTP {resp.status_code}")
        return page, resp.content


# ──────────────────────────────────────────────────────────────────────────────
# Main entry-point
# ──────────────────────────────────────────────────────────────────────────────
def download_and_stream_to_pdf_concurrent(
    reader_url: str, *, max_workers: int = 16
) -> BytesIO:
    """
    Build a PDF from a FullBookReader URL with concurrent downloads
    and no temporary files.  Returns a BytesIO you can hand to Flask's
    `send_file`, upload to S3, etc.
    """
    total_pages, ext, base_url = _parse_reader_url(reader_url)

    pdf_buffer = BytesIO()
    pdf_canvas = canvas.Canvas(pdf_buffer, pageCompression=1)

    next_page_to_write = 1
    ready: dict[int, bytes] = {}  # holds out-of-order downloads

    with cf.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_page, p, base_url, ext): p
            for p in range(1, total_pages + 1)
        }

        # As each download finishes …
        for fut in cf.as_completed(futures):
            page_num, img_bytes = fut.result()
            ready[page_num] = img_bytes

            # … write all consecutive pages that are now available
            while next_page_to_write in ready:
                data = ready.pop(next_page_to_write)
                with Image.open(BytesIO(data)) as img:
                    if img.mode != "RGB":
                        img = img.convert("RGB")
                    w, h = img.size
                    pdf_canvas.setPageSize((w, h))
                    pdf_canvas.drawInlineImage(img, 0, 0, w, h)
                    pdf_canvas.showPage()
                next_page_to_write += 1

    pdf_canvas.save()
    pdf_buffer.seek(0)
    return pdf_buffer
