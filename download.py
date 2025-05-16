import requests
from io import BytesIO
from PIL import Image
from urllib.parse import urlparse, parse_qs, unquote, urljoin
import re

def _make_base_img_url(reader_url: str, rel_path: str) -> str:
    parsed = urlparse(reader_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"
    if not rel_path.startswith("/"):
        rel_path = "/" + rel_path
    return urljoin(site_root, rel_path)

def download_images_in_memory(reader_url: str):
    q = parse_qs(urlparse(reader_url).query)
    total_pages = int(q.get("TotalPage", [0])[0])
    ext = q.get("ext", ["jpg"])[0]
    encoded_path = q.get("Url", [""])[0]
    if not encoded_path or total_pages == 0:
        raise ValueError("Reader URL missing 'Url' or 'TotalPage'")
    
    rel_path = unquote(encoded_path).lstrip("/")
    if not rel_path.endswith("/"):
        rel_path += "/"
    base_img_url = _make_base_img_url(reader_url, rel_path)

    session = requests.Session()
    images = []
    for page in range(1, total_pages + 1):
        fname = f"{page:06d}.{ext}"
        img_url = urljoin(base_img_url, fname)
        resp = session.get(img_url, timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Failed to download page {page}: status {resp.status_code}")
        img_bytes = BytesIO(resp.content)
        img = Image.open(img_bytes)
        if img.mode != "RGB":
            img = img.convert("RGB")
        images.append(img)
    return images

def images_to_pdf_bytes(images):
    pdf_bytes = BytesIO()
    first, *rest = images
    first.save(pdf_bytes, format="PDF", save_all=True, append_images=rest)
    pdf_bytes.seek(0)
    return pdf_bytes

def download_and_build_pdf_in_memory(reader_url: str):
    images = download_images_in_memory(reader_url)
    pdf_buffer = images_to_pdf_bytes(images)
    return pdf_buffer
