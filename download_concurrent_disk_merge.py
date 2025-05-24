import os
import requests
from io import BytesIO
from PIL import Image
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _make_base_img_url(reader_url: str, rel_path: str) -> str:
    parsed = urlparse(reader_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"
    if not rel_path.startswith("/"):
        rel_path = "/" + rel_path
    return urljoin(site_root, rel_path)

def _create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0"
    })
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def _download_single_image(session, page: int, img_url: str):
    resp = session.get(img_url, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Failed to download page {page}: status {resp.status_code}")
    img = Image.open(BytesIO(resp.content))
    if img.mode != "RGB":
        img = img.convert("RGB")
    return page, img

def download_and_stream_to_pdf_concurrent(reader_url: str, output_path: str = "output.pdf", max_workers: int = 8):
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
    session = _create_session()

    urls = [(page, urljoin(base_img_url, f"{page:06d}.{ext}")) for page in range(1, total_pages + 1)]

    # Download images concurrently
    page_image_map = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_download_single_image, session, page, url): page for page, url in urls}
        for future in as_completed(futures):
            page = futures[future]
            try:
                pg, img = future.result()
                page_image_map[pg] = img
            except Exception as e:
                raise RuntimeError(f"Error downloading page {page}: {e}")

    # Assemble PDF in page order
    ordered_images = [page_image_map[pg] for pg in sorted(page_image_map)]
    pdf_buffer = BytesIO()
    ordered_images[0].save(pdf_buffer, format="PDF", save_all=True, append_images=ordered_images[1:])
    pdf_buffer.seek(0)
    return pdf_buffer
    

# download_and_stream_to_pdf_concurrent(
#     "https://thuvien.hmu.edu.vn/pages/cms/FullBookReader.aspx?Url=%2Fpages%2Fcms%2FTempDir%2Fbooks%2F202006150953-bc775b84-b0d0-49c1-b579-eddd928c30ee%2F%2FFullPreview&TotalPage=162&ext=jpg",
#     output_path="downloads/mybook.pdf"
# )