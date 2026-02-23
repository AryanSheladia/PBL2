from __future__ import annotations

import shutil
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from src.parsers.universal_parser import parse_any  # uses your existing parser


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

        # Top controls
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        self.upload_btn = ttk.Button(top, text="Upload File", command=self.upload_file)
        self.upload_btn.pack(side="left")

        ttk.Label(top, text="   Select File:").pack(side="left")

        self.file_var = tk.StringVar()
        self.file_dropdown = ttk.Combobox(top, textvariable=self.file_var, state="readonly", width=60)
        self.file_dropdown.pack(side="left", padx=8)

        self.refresh_btn = ttk.Button(top, text="Refresh", command=self.refresh_files)
        self.refresh_btn.pack(side="left")

        self.parse_btn = ttk.Button(top, text="Parse", command=self.parse_selected)
        self.parse_btn.pack(side="left", padx=8)

        # Output area
        mid = ttk.Frame(self, padding=10)
        mid.pack(fill="both", expand=True)

        self.output = tk.Text(mid, wrap="word", font=("Consolas", 10))
        self.output.pack(fill="both", expand=True)

        self.refresh_files()

    def log(self, msg: str):
        self.output.insert("end", msg + "\n")
        self.output.see("end")

    def refresh_files(self):
        files = [p.name for p in DATA_DIR.iterdir() if p.is_file() and p.suffix.lower() in [".pdf", ".docx", ".txt", ".md", ".csv"]]
        files.sort()
        self.file_dropdown["values"] = files

        if files:
            if self.file_var.get() not in files:
                self.file_var.set(files[-1])  # select latest alphabetically
        else:
            self.file_var.set("")

    def upload_file(self):
        file_path = filedialog.askopenfilename(
            title="Choose a file to upload",
            filetypes=[
                ("Supported files", "*.pdf *.docx *.txt *.md *.csv"),
                ("PDF", "*.pdf"),
                ("Word", "*.docx"),
                ("Text", "*.txt *.md"),
                ("CSV", "*.csv"),
                ("All files", "*.*"),
            ],
        )
        if not file_path:
            return

        src = Path(file_path)
        if not src.exists():
            messagebox.showerror("Error", "File not found.")
            return

        dest = DATA_DIR / src.name
        try:
            shutil.copy2(src, dest)
        except Exception as e:
            messagebox.showerror("Copy failed", str(e))
            return

        self.log(f"✅ Uploaded: {src.name}  →  data/{src.name}")
        self.refresh_files()
        self.file_var.set(dest.name)

    def parse_selected(self):
        filename = self.file_var.get().strip()
        if not filename:
            messagebox.showwarning("No file selected", "Upload a file or select one from the dropdown.")
            return

        doc_path = DATA_DIR / filename
        if not doc_path.exists():
            messagebox.showerror("Error", f"File missing: {doc_path}")
            self.refresh_files()
            return

        self.log(f"\n--- Parsing: {filename} ---")
        try:
            parsed = parse_any(doc_path)  # your universal parser
        except Exception as e:
            messagebox.showerror("Parse failed", str(e))
            self.log(f"❌ Parse failed: {e}")
            return

        out_path = LOGS_DIR / f"{doc_path.stem}.parsed.json"
        out_path.write_text(parsed.model_dump_json(indent=2), encoding="utf-8")

        self.log(f"Doc type: {parsed.doc_type}")
        self.log(f"Template: {parsed.template_name}")
        self.log("Sections:")
        for s in parsed.sections:
            self.log(f" - {s.section_id:24} conf={s.confidence:.2f} anchor={s.anchor}")

        self.log(f"✅ Saved JSON → logs/{out_path.name}")
        self.log("--- Done ---\n")


if __name__ == "__main__":
    app = ParserUI()
    app.mainloop()