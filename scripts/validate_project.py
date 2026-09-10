from pathlib import Path
import ast
import csv

ROOT = Path(__file__).resolve().parents[1]
required = [
    "src/ola_pipeline.py",
    "sql/dashboard_dataset.sql",
    "powerbi/measures.dax",
    "sample_data/trips.csv",
    "sample_data/cities.csv",
    "docs/powerbi-implementation.png",
]
missing = [p for p in required if not (ROOT / p).is_file()]
if missing:
    raise SystemExit(f"Missing required files: {missing}")
ast.parse((ROOT / "src/ola_pipeline.py").read_text(encoding="utf-8"))
for name in ("trips.csv", "cities.csv"):
    with (ROOT / "sample_data" / name).open(encoding="utf-8", newline="") as handle:
        if not list(csv.DictReader(handle)):
            raise SystemExit(f"No sample rows in {name}")
print("Project structure, Python syntax, and sample data are valid.")

