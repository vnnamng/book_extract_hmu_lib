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
# Helpers
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


def _download_single_image(session: requests.Session, img_url: str, out_path: Path):
    """Download one image and write it straight to disk."""
    resp = session.get(img_url, timeout=30)
    resp.raise_for_status()

    img = Image.open(BytesIO(resp.content))
    if img.mode != "RGB":
        img = img.convert("RGB")
    img.save(out_path, "JPEG")                   # keep a uniform extension


# ──────────────────────────────────────────
# Public API
# ──────────────────────────────────────────
def download_images_to_dir(reader_url: str, out_dir: str | Path, max_workers: int = 8):
    """
    Stream every page image of *reader_url* into *out_dir*.
    The resulting files are named 000001.jpg, 000002.jpg, ...
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
            ): i
            for i, url in enumerate(urls)
        }
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                fut.result()
            except Exception as e:
                raise RuntimeError(f"Error on page {idx + 1}: {e}") from e

    print(f"✓ Downloaded {total_pages} images → {out_dir}")

from fpdf import FPDF

def compile_dir_to_pdf(images_dir: str | Path, output_pdf: str | Path):
    images_dir = Path(images_dir)
    files = sorted(
        p for p in images_dir.iterdir()
        if p.suffix.lower() in {".jpg", ".jpeg", ".png"}
    )
    if not files:
        raise ValueError(f"No images found in {images_dir}")

    pdf = FPDF(unit="pt")           # use points so px == pt at 72 dpi
    for img_path in files:
        with Image.open(img_path) as im:
            w, h = im.size          # dimensions in px
        pdf.add_page(format=(w, h))
        # x = y = 0, w = page width, h = page height (no scaling artefacts)
        pdf.image(str(img_path), x=0, y=0, w=w, h=h)

    pdf.output(str(output_pdf))
    print(f"✓ PDF created → {output_pdf}")


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
