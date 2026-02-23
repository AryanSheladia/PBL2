from pathlib import Path
from src.parsers.universal_parser import parse_any

if __name__ == "__main__":
    doc = Path("data/test.pdf")  # change to .docx/.txt/.csv
    parsed = parse_any(doc)

    Path("logs").mkdir(exist_ok=True)
    out = Path("logs/parsed.json")
    out.write_text(parsed.model_dump_json(indent=2), encoding="utf-8")

    print("Doc type:", parsed.doc_type)
    print("Template:", parsed.template_name)
    print("\nSections:")
    for s in parsed.sections:
        print(f"- {s.section_id:24} conf={s.confidence:.2f} anchor={s.anchor}")
    print("\nSaved ->", out)