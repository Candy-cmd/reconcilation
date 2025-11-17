from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import os
import tempfile
import pandas as pd
from recon import run_reconciliation
from datetime import datetime

app = Flask(__name__)
app.secret_key = "secure_secret_key"

UPLOAD_FOLDER = tempfile.gettempdir()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/process", methods=["POST"])
def process():
    try:
        admin_file = request.files.get("admin_file")
        echeque_file = request.files.get("echeque_file")
        yono_file = request.files.get("yono_file")
        selected_date = request.form.get("selected_date")

        if not (admin_file and echeque_file and yono_file and selected_date):
            flash("Please upload all files and select a date.")
            return redirect(url_for("index"))

        output_dir = os.path.join(UPLOAD_FOLDER, "recon_" + datetime.now().strftime("%Y%m%d_%H%M%S"))
        os.makedirs(output_dir, exist_ok=True)

        admin_path = os.path.join(output_dir, admin_file.filename)
        echeque_path = os.path.join(output_dir, echeque_file.filename)
        yono_path = os.path.join(output_dir, yono_file.filename)

        admin_file.save(admin_path)
        echeque_file.save(echeque_path)
        yono_file.save(yono_path)

        results = run_reconciliation(admin_path, echeque_path, yono_path, selected_date, output_dir)

        preview_data = {
            name: df.head(20).to_html(classes="table table-striped table-bordered", index=False)
            for name, df in results.items()
            if isinstance(df, pd.DataFrame) and not df.empty
        }

        return render_template("result.html",
                               date=selected_date,
                               previews=preview_data,
                               excel=os.path.basename(results["Excel_File"]),
                               pdf=os.path.basename(results["PDF_File"]),
                               folder=output_dir)
    except Exception as e:
        flash(f"Error: {str(e)}")
        return redirect(url_for("index"))

@app.route("/download/<path:folder>/<path:filename>")
def download(folder, filename):
    full_path = os.path.join(folder, filename)
    if os.path.exists(full_path):
        return send_file(full_path, as_attachment=True)
    else:
        flash("File not found.")
        return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True, port=5000)
