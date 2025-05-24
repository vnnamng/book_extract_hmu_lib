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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/90 Safari/537.36"
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

def _download_single_image(session, img_url: str):
    resp = session.get(img_url, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Failed to download {img_url}: status {resp.status_code}")
    img = Image.open(BytesIO(resp.content))
    if img.mode != "RGB":
        img = img.convert("RGB")
    return img

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
    session = _create_session()

    urls = [urljoin(base_img_url, f"{page:06d}.{ext}") for page in range(1, total_pages + 1)]

    images = [None] * total_pages
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_download_single_image, session, url): i for i, url in enumerate(urls)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                images[idx] = future.result()
            except Exception as e:
                raise RuntimeError(f"Error on page {idx + 1}: {e}")
    return images

def download_and_stream_to_pdf(reader_url: str, output_path: str = "output.pdf"):
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

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    pdf_initialized = False

    for page in range(1, total_pages + 1):
        img_url = urljoin(base_img_url, f"{page:06d}.{ext}")
        try:
            img = _download_single_image(session, img_url)
            if not pdf_initialized:
                img.save(output_path, format="PDF", save_all=True, append_images=[])
                pdf_initialized = True
            else:
                # Append to existing PDF by re-opening, merging, and overwriting (intermediate temp handling needed)
                with BytesIO() as buffer:
                    img.save(buffer, format="PDF")
                    buffer.seek(0)
                    with open(output_path, "ab") as f:
                        f.write(buffer.read())
        except Exception as e:
            print(f"Failed to process page {page}: {e}")

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

def images_to_pdf_file(images, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    images[0].save(output_path, format="PDF", save_all=True, append_images=images[1:])
    print(f"PDF saved to: {output_path}")

def download_and_save_pdf(reader_url: str, output_path: str = "output.pdf"):
    images = download_images_in_memory(reader_url)
    images_to_pdf_file(images, output_path)

# download_and_save_pdf("https://thuvien.hmu.edu.vn/pages/cms/FullBookReader.aspx?Url=%2Fpages%2Fcms%2FTempDir%2Fbooks%2F202006150953-bc775b84-b0d0-49c1-b579-eddd928c30ee%2F%2FFullPreview&TotalPage=162&ext=jpg#page/4/mode/2up", "downloads/mybook.pdf")


