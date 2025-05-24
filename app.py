from flask import Flask, render_template, request, redirect, url_for, send_from_directory, flash
from pathlib import Path
import os
from flask_cors import CORS

from download_concurrent import  download_and_save_pdf # Adjust import if needed

app = Flask(__name__)
CORS(app)
app.secret_key = "lenhatanh"
BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)


from flask import send_file

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        url = request.form.get("reader_url")
        if not url:
            flash("Please provide a valid FullBookReader URL.")
            return redirect(url_for("index"))

        folder_name = request.form.get("folder_name", "book")
        pdf_filename = folder_name + ".pdf"

        try:
            pdf_path = Path("/tmp/{pdf_filename}")
            download_and_save_pdf(url, images_dir="/tmp/pages", output_pdf=pdf_path)
            # Send the file in the output path as a downloadable file response
            return send_file(
                pdf_path,
                mimetype="application/pdf",
                as_attachment=True,
                download_name=pdf_filename,
            )
        except Exception as e:
            flash(f"Error: {str(e)}")
            return redirect(url_for("index"))

    return render_template("index.html")



from flask import after_this_request
import threading
import time
import shutil

@app.route("/download/<folder>/<filename>")
def download_file(folder, filename):
    file_path = DOWNLOAD_DIR / folder / filename

    @after_this_request
    def remove_folder(response):
        def delayed_delete():
            time.sleep(5)  # Wait for 5 seconds to ensure file is sent
            try:
                shutil.rmtree(DOWNLOAD_DIR / folder)
                print(f"Deleted folder: {folder}")
            except Exception as e:
                print(f"Failed to delete {folder}: {e}")
        threading.Thread(target=delayed_delete).start()
        return response

    return send_from_directory(DOWNLOAD_DIR / folder, filename, as_attachment=True)