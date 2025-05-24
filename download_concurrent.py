import os
from pathlib import Path
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ──────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────
def _make_base_img_url(reader_url: str, rel_path: str) -> str:
    p = urlparse(reader_url)
    root = f"{p.scheme}://{p.netloc}"
    return urljoin(root, rel_path if rel_path.startswith("/") else "/" + rel_path)


def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "Chrome/90 Safari/537.36"
    )
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _download_single_image(
    session: requests.Session,
    img_url: str,
    out_path: Path,
    max_edge: int | None = None,
    jpeg_quality: int = 85,
):
    """
    Download one image, optionally down-scale so that its longest
    side ≤ *max_edge*, and save to JPEG with *jpeg_quality*.
    """
    resp = session.get(img_url, timeout=30)
    resp.raise_for_status()

    img = Image.open(BytesIO(resp.content))
    if img.mode != "RGB":
        img = img.convert("RGB")

    if max_edge:                      # ↓ keep aspect ratio, high-quality filter
        img.thumbnail((max_edge, max_edge), Image.LANCZOS)

    img.save(out_path, "JPEG", quality=jpeg_quality, optimize=True)


# ──────────────────────────────────────────
# Public API
# ──────────────────────────────────────────
def download_images_to_dir(
    reader_url: str,
    out_dir: str | Path,
    *,
    max_workers: int = 8,
    downscale_max_edge: int | None = None,
    jpeg_quality: int = 85,
    # Automatic policy:
    auto_downscale_pages: int = 400,
    auto_downscale_edge: int = 1400,
):
    """
    Stream every page image of *reader_url* into *out_dir*.
    Files are named 000001.jpg, 000002.jpg, ...

    Parameters
    ----------
    downscale_max_edge:
        Longest side (px) for resized pages.
        • None  → no resize
        • -1    → auto-decide based on *auto_downscale_pages*
    jpeg_quality :
        95 (max) ↔ 1 (tiny/ugly); 80-90 is usually sweet-spot.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    q = parse_qs(urlparse(reader_url).query)
    total_pages = int(q.get("TotalPage", [0])[0])
    ext = q.get("ext", ["jpg"])[0]
    rel_path = unquote(q.get("Url", [""])[0]).lstrip("/")
    if not rel_path.endswith("/"):
        rel_path += "/"
    base = _make_base_img_url(reader_url, rel_path)

    # Decide whether we should down-scale automatically
    if downscale_max_edge is None and total_pages >= auto_downscale_pages:
        downscale_max_edge = auto_downscale_edge

    session = _create_session()
    urls = [
        urljoin(base, f"{page:06d}.{ext}") for page in range(1, total_pages + 1)
    ]

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _download_single_image,
                session,
                url,
                out_dir / f"{i+1:06d}.jpg",
                downscale_max_edge,
                jpeg_quality,
            ): i
            for i, url in enumerate(urls)
        }
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                fut.result()
            except Exception as e:
                raise RuntimeError(f"Error on page {idx + 1}: {e}") from e

    print(
        f"✓ Downloaded {total_pages} images"
        + (" (down-scaled)" if downscale_max_edge else "")
        + f" → {out_dir}"
    )

def download_and_save_pdf(
    reader_url: str,
    images_dir: str | Path = "pages",
    output_pdf: str | Path = "book.pdf",
):
    """
    End-to-end helper: download images → dir, then compile → PDF.
    """
    download_images_to_dir(reader_url, images_dir)
    compile_dir_to_pdf(images_dir, output_pdf)
