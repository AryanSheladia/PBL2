from __future__ import annotations

import shutil
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.services.document_service import create_document, update_document_status, get_document_by_filename
from src.services.parser_service import store_parsed_document
from src.parsers.universal_parser import parse_any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"


class ParserUI(tk.Tk):

    def __init__(self):
        super().__init__()

        self.title("PBL_2 - Universal Parser")
        self.geometry("900x600")

        DATA_DIR.mkdir(exist_ok=True)
        LOGS_DIR.mkdir(exist_ok=True)

        # ---------------- Top Controls ---------------- #

        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        self.upload_btn = ttk.Button(top, text="Upload File", command=self.upload_file)
        self.upload_btn.pack(side="left")

        ttk.Label(top, text="   Select File:").pack(side="left")

        self.file_var = tk.StringVar(value="Select file")

        self.file_dropdown = ttk.Combobox(
            top,
            textvariable=self.file_var,
            state="readonly",
            width=60
        )
        self.file_dropdown.pack(side="left", padx=8)

        self.refresh_btn = ttk.Button(top, text="Refresh", command=self.refresh_files)
        self.refresh_btn.pack(side="left")

        self.parse_btn = ttk.Button(top, text="Parse", command=self.parse_selected)
        self.parse_btn.pack(side="left", padx=8)

        # ---------------- Output Area ---------------- #

        mid = ttk.Frame(self, padding=10)
        mid.pack(fill="both", expand=True)

        self.output = tk.Text(mid, wrap="word", font=("Consolas", 10))
        self.output.pack(fill="both", expand=True)

        self.refresh_files()

    # ------------------------------------------------ #

    def log(self, msg: str):
        self.output.insert("end", msg + "\n")
        self.output.see("end")

    # ------------------------------------------------ #

    def refresh_files(self):

        files = [f.name for f in DATA_DIR.iterdir() if f.is_file()]

        self.file_dropdown["values"] = files

        # reset dropdown
        self.file_var.set("Select file")

        # clear output window
        self.output.delete("1.0", "end")

    # ------------------------------------------------ #

    def upload_file(self):

        filepath = filedialog.askopenfilename()

        if not filepath:
            return

        src = Path(filepath)
        dest = DATA_DIR / src.name

        shutil.copy(src, dest)

        # Save metadata in Mongo
        create_document(
            file_name=dest.name,
            file_size=dest.stat().st_size,
            storage_path=str(dest)
        )

        self.log(f"Uploaded {dest.name}")

        self.refresh_files()

    # ------------------------------------------------ #

    def parse_selected(self):

        filename = self.file_var.get().strip()

        if filename == "Select file":
            messagebox.showwarning("No file selected", "Please select a file.")
            return

        doc_path = DATA_DIR / filename

        if not doc_path.exists():
            messagebox.showerror("Error", f"File missing: {doc_path}")
            return

        self.log(f"\n--- Parsing: {filename} ---")

        try:

            # Fetch document from DB
            doc = get_document_by_filename(filename)

            if not doc:
                messagebox.showerror("Error", "Document not found in DB")
                return

            document_id = doc["_id"]

            # Update status
            update_document_status(document_id, "parsing", "parsing")

            # Run parser
            parsed = parse_any(doc_path)

            # Store parsed result
            store_parsed_document(document_id, parsed)

            # Update final status
            update_document_status(document_id, "parsed", "parsed")

        except Exception as e:

            self.log(f"❌ Parse failed: {e}")
            messagebox.showerror("Parse failed", str(e))
            return

        # Display parsed output

        self.log(f"Doc type: {parsed.doc_type}")
        self.log(f"Template: {parsed.template_name}")
        self.log("Sections:")

        for s in parsed.sections:
            self.log(
                f" - {s.section_id:24} "
                f"conf={getattr(s,'confidence',0):.2f} "
                f"anchor={getattr(s,'anchor',None)}"
            )

        self.log("✅ Parsed output stored in MongoDB")
        self.log("--- Done ---\n")


# ---------------------------------------------------- #

if __name__ == "__main__":
    app = ParserUI()
    app.mainloop()