import os
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote, urljoin

import requests
from urllib.parse import urlparse, urljoin

def _make_base_img_url(reader_url: str, rel_path: str) -> str:
    """
    Return   https://thuvien.hmu.edu.vn/<rel_path>/   (trailing slash kept).

    Works even if `rel_path` itself contains a double slash (“//FullPreview…”).
    """
    parsed = urlparse(reader_url)
    site_root = f"{parsed.scheme}://{parsed.netloc}"
    if not rel_path.startswith("/"):
        rel_path = "/" + rel_path            # make it absolute
    return urljoin(site_root, rel_path)      # urljoin just concatenates here


def download_reader_images(reader_url: str, dest_folder: str = "book_pages") -> None:
    """
    Download every page image from a thuvien.hmu.edu.vn FullBookReader link.

    Parameters
    ----------
    reader_url : str
        The URL you get when you open the online book reader
        (e.g. …/FullBookReader.aspx?Url=%2Fpages%2F…%2FFullPreview&TotalPage=180&ext=jpg#page/2/mode/2up)
    dest_folder : str, optional
        Where to save the images (created if it doesn’t exist), by default "book_pages".

    Notes
    -----
    • If the site requires you to be logged in, launch this script in the same browser session  
      (e.g. with `requests_html` or by exporting cookies) or supply your cookies in the request.  
    • Images are named **000001.jpg … 000180.jpg** (zero‑padded to 6 digits).
    """
    # — extract the key query parameters —────────────────────────────────────────
    q = parse_qs(urlparse(reader_url).query)
    total_pages = int(q.get("TotalPage", [0])[0])
    ext = q.get("ext", ["jpg"])[0]
    encoded_path = q.get("Url", [""])[0]
    if not encoded_path or total_pages == 0:
        raise ValueError("Reader URL is missing either 'Url' or 'TotalPage'")

    # — build the base path that hosts the images —──────────────────────────────
    #   Url param looks like "/pages/cms/TempDir/books/<id>/FullPreview"
    rel_path = unquote(encoded_path).lstrip("/")
    if not rel_path.endswith("/"):
        rel_path += "/"
    base_img_url = _make_base_img_url(reader_url, rel_path)
    # — prepare output directory —───────────────────────────────────────────────
    Path(dest_folder).mkdir(parents=True, exist_ok=True)

    # — fetch each image —───────────────────────────────────────────────────────
    session = requests.Session()
    for page in range(1, total_pages + 1):
        fname = f"{page:06d}.{ext}"
        img_url = urljoin(base_img_url, fname)
        out_path = Path(dest_folder) / fname

        if out_path.exists():
            print(f"{fname} already exists — skipped")
            continue

        resp = session.get(img_url, stream=True, timeout=30)
        print(f"fetching {fname} from {img_url}")
        if resp.ok:
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(16 * 1024):
                    f.write(chunk)
            print(f"downloaded {fname}")
        else:
            print(f"failed {fname} status {resp.status_code}")


from pathlib import Path
import glob
import re
from PIL import Image  # pip install pillow


def images_to_pdf(
    img_folder: str | Path,
    output_pdf: str | Path = "book.pdf",
    ext: str = "jpg",
    dpi: int = 300,
) -> None:
    """
    Collate every <pageN>.<ext> image in `img_folder` into a single PDF.

    Parameters
    ----------
    img_folder : str | Path
        Directory that already contains the downloaded page images.
    output_pdf : str | Path, optional
        Target PDF name (can include a path).  Defaults to "book.pdf"
        and is written *next to* the images unless you pass an absolute path.
    ext : str, optional
        File‑extension of the images ("jpg", "png", …).  Defaults to "jpg".
    dpi : int, optional
        Metadata DPI to embed in the PDF (doesn’t resample).  Defaults to 300.
    """
    img_folder = Path(img_folder)
    output_pdf = Path(output_pdf)
    pattern = str(img_folder / f"*.{ext}")

    # — gather & numerically sort the page files —──────────────────────────────
    rx_page_num = re.compile(r"(\d+)\." + re.escape(ext) + r"$")
    pages = sorted(
        glob.glob(pattern),
        key=lambda p: int(rx_page_num.search(p).group(1)) if rx_page_num.search(p) else 0,
    )
    if not pages:
        raise FileNotFoundError(f"No *.{ext} files in {img_folder}")

    # — open images and ensure RGB —────────────────────────────────────────────
    pil_pages = []
    for p in pages:
        img = Image.open(p)
        if img.mode != "RGB":
            img = img.convert("RGB")
        pil_pages.append(img)

    # — save to one PDF —───────────────────────────────────────────────────────
    first, *rest = pil_pages
    first.save(
        output_pdf,
        "PDF",
        save_all=True,
        append_images=rest,
        resolution=dpi,
    )
    print(f"wrote {output_pdf} with {len(pages)} pages")

import shutil

def download_and_build_pdf(
    reader_url: str,
    dest_folder: str = "book_pages",
    pdf_name: str = "book.pdf",
) -> None:
    """One‑shot helper: grab the images then stitch them into a PDF."""
    dest_path = Path(dest_folder)

    # Clear folder if it already exists
    if dest_path.exists():
        shutil.rmtree(dest_path)  # Remove the folder and its contents
    dest_path.mkdir(parents=True, exist_ok=True)  # Recreate the folder

    # Proceed with download and PDF creation
    download_reader_images(reader_url, dest_folder)
    images_to_pdf(dest_folder, Path(dest_folder) / pdf_name)
# 