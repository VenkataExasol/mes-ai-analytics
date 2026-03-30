import json
from pathlib import Path

import pandas as pd

DATA_PATH = Path(__file__).resolve().parent.parent / "data"
OUTPUT_PATH = Path(__file__).resolve().parent / "schema_output.json"

class SchemaDetectionAgent:

    def __init__(self):
        self.schema_rules = {
            "event": ["event_type", "timestamp", "duration", "machine"],
            "kpi": ["availability", "performance", "quality", "oee"],
            "production": ["planned_hours", "run_hours", "units_produced"],
            "time": ["total_hours", "operating_hours", "oee"],
            "quality": ["defective", "downtime", "reason"]
        }

    def detect(self, df):
        columns = [str(c).lower() for c in df.columns]
        scores = {}

        for schema in self.schema_rules:
            score = 0
            for keyword in self.schema_rules[schema]:
                if any(keyword in col for col in columns):
                    score += 1
            scores[schema] = score

        best_schema = max(scores, key=scores.get)
        confidence = scores[best_schema] / (len(self.schema_rules[best_schema]) + 1)

        if confidence < 0.3:
            best_schema = "unknown"

        return best_schema, round(confidence, 2), scores


def run():
    agent = SchemaDetectionAgent()
    results = []

    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Data folder not found: {DATA_PATH}")

    for file_path in sorted(DATA_PATH.iterdir()):
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in {".xlsx", ".xls"}:
            continue

        try:
            df = pd.read_excel(file_path, engine="openpyxl")
        except Exception as e:
            print(f"Skipping {file_path.name}: {e}")
            continue

        schema, confidence, scores = agent.detect(df)

        results.append({
            "file": file_path.name,
            "schema": schema,
            "confidence": confidence
        })

        print(f"{file_path.name} → {schema} ({confidence})")

    with OUTPUT_PATH.open("w", encoding="utf-8") as out:
        json.dump(results, out, indent=4)


if __name__ == "__main__":
    run()
