import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_OUTPUT_PATH = BASE_DIR / "schema_output.json"
MAPPING_OUTPUT_PATH = BASE_DIR / "mapping_output.json"

def generate_mapping(schema):
    mapping = {
        "event": "event",
        "kpi": "kpi",
        "production": "production",
        "time": "time",
        "quality": "quality"
    }
    return mapping.get(schema, "unknown")


def run():
    with SCHEMA_OUTPUT_PATH.open(encoding="utf-8") as f:
        data = json.load(f)

    output = []

    for item in data:
        mapping_type = generate_mapping(item["schema"])

        output.append({
            "file": item["file"],
            "schema": item["schema"],
            "mapping_type": mapping_type
        })

        print(f"{item['file']} → {mapping_type}")

    with MAPPING_OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=4)


if __name__ == "__main__":
    run()
