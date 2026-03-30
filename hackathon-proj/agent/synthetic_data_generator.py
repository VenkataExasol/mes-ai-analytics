"""
Synthetic Data Generator - Generates data matching actual plant file structures.
Reads column structure from actual Excel files in /data/, generates synthetic data in same format.
This ensures synthetic data goes through identical transformation as real data.
"""

import json
import os
import random
import datetime as dt
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR.parent / "data"
MAPPING_OUTPUT_PATH = BASE_DIR / "mapping_output.json"
SYNTHETIC_DATA_PATH = BASE_DIR.parent / "synthetic_data"


def _load_env_file() -> None:
    env_path = BASE_DIR.parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _synthetic_config() -> dict[str, int | float | None]:
    """Get synthetic data configuration from environment."""
    seed_raw = os.getenv("SYNTHETIC_SEED", "").strip()
    seed_value = int(seed_raw) if seed_raw else None
    return {
        "rows_per_file": max(1, _env_int("SYNTHETIC_ROWS_PER_FILE", 50)),
        "seed": seed_value,
    }


def _extract_plant_id(file_name: str) -> str:
    import re
    match = re.search(r"(P\d{2})", file_name, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    stem = Path(file_name).stem.upper()
    return stem[:10]


def _bounded(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _read_actual_file(file_path: Path) -> list[str]:
    """Read actual Excel file to get column structure."""
    try:
        df = pd.read_excel(file_path, engine="openpyxl", nrows=1)
        columns = [str(c).lower() for c in df.columns]
        return columns
    except Exception as e:
        print(f"   ⚠️  Could not extract columns from {file_path.name}: {e}")
        return []


def _generate_synthetic_record(
    columns: list[str],
    file_name: str,
    row_idx: int,
    base_date: dt.date,
    rng: random.Random,
) -> dict:
    """Generate synthetic record matching actual file columns."""
    plant_id = _extract_plant_id(file_name)
    current_date = base_date + dt.timedelta(days=row_idx)
    record = {}
    
    # Generate values matching column names and types
    for col in columns:
        col_lower = col.lower()
        
        # Date/Timestamp columns
        if any(x in col_lower for x in ["date", "timestamp", "time"]):
            if "time" in col_lower and "date" not in col_lower:
                record[col] = f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"
            else:
                record[col] = current_date.isoformat()
        
        # Plant/Machine identifiers
        elif "plant" in col_lower:
            record[col] = plant_id
        elif "machine" in col_lower or "equipment" in col_lower:
            record[col] = f"M-{rng.randint(1, 20):02d}"
        
        # Percentages
        elif "pct" in col_lower or "percent" in col_lower or col_lower.endswith("%"):
            record[col] = round(rng.uniform(50, 99), 2)
        
        # Hours/Time duration
        elif any(x in col_lower for x in ["hour", "hours"]):
            record[col] = round(rng.uniform(5, 12), 2)
        
        # Seconds/duration
        elif any(x in col_lower for x in ["sec", "second", "duration"]):
            record[col] = int(rng.uniform(300, 3600))
        
        # Units/quantities
        elif any(x in col_lower for x in ["unit", "produced", "defective", "good", "bad", "reject", "scrap"]):
            if "defective" in col_lower or "bad" in col_lower or "scrap" in col_lower or "reject" in col_lower:
                record[col] = int(rng.uniform(10, 500))
            else:
                record[col] = int(rng.uniform(1000, 8000))
        
        # Boolean/categorical
        elif any(x in col_lower for x in ["type", "event", "status", "reason", "state"]):
            if "event_type" in col_lower or "type" in col_lower:
                record[col] = rng.choice(["RUN", "IDLE", "STOP", "SETUP", "MAINTENANCE"])
            elif "reason" in col_lower:
                record[col] = rng.choice(["maintenance", "quality_issue", "setup", "material", "equipment"])
            elif "status" in col_lower or "state" in col_lower:
                record[col] = rng.choice(["Active", "Idle", "Error", "Stopped"])
            else:
                record[col] = rng.choice(["A", "B", "C"])
        
        # OEE or ratios
        elif col_lower == "oee" or "oee" in col_lower:
            record[col] = round(rng.uniform(0.3, 0.95), 4)
        
        # Downtime/efficiency numbers
        elif any(x in col_lower for x in ["downtime", "efficiency", "rate"]):
            record[col] = round(rng.uniform(10, 120), 2)
        
        # Default: numeric with sensible range
        else:
            record[col] = round(rng.uniform(0, 100), 2)
    
    return record


def generate():
    """Generate synthetic data matching actual file structures from /data/."""
    _load_env_file()
    
    # Create synthetic data directory
    SYNTHETIC_DATA_PATH.mkdir(exist_ok=True)
    
    # Load mapping
    with MAPPING_OUTPUT_PATH.open(encoding="utf-8") as f:
        mapping_data = json.load(f)
    
    synth_cfg = _synthetic_config()
    rows_per_file = int(synth_cfg["rows_per_file"])
    seed = synth_cfg["seed"]
    rng = random.Random(seed)
    
    print(f"🧪 Generating synthetic data (rows_per_file={rows_per_file}, seed={seed})")
    print(f"📚 Reading actual file structures from {DATA_PATH}")
    print()
    
    generated = 0
    failed = 0
    total_records = 0
    
    for item in mapping_data:
        file = item.get("file", "")
        schema = str(item.get("schema", "unknown")).strip().lower()
        
        try:
            # Read actual file to get column structure
            actual_file_path = DATA_PATH / file
            if not actual_file_path.exists():
                print(f"⚠️  Source file not found: {file}")
                failed += 1
                continue
            
            # Get actual column structure from the real file
            actual_columns = _read_actual_file(actual_file_path)
            if not actual_columns:
                print(f"⚠️  Could not extract columns from {file}")
                failed += 1
                continue
            
            # Generate synthetic records matching actual structure
            records = []
            base_date = dt.date.today() - dt.timedelta(days=rows_per_file - 1)
            
            for row_idx in range(rows_per_file):
                record = _generate_synthetic_record(
                    columns=actual_columns,
                    file_name=file,
                    row_idx=row_idx,
                    base_date=base_date,
                    rng=rng,
                )
                records.append(record)
            
            # Save as JSONL (JSON Lines) for streaming
            output_path = SYNTHETIC_DATA_PATH / file.replace(".xlsx", ".jsonl").replace(".xls", ".jsonl")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write records as streaming JSON lines
            with open(output_path, "w", encoding="utf-8") as f:
                for record in records:
                    f.write(json.dumps(record) + "\n")
            
            total_records += len(records)
            print(f"✅ {file:40s} ({schema:10s}) → {len(records):2d} records")
            
            generated += 1
            
        except Exception as e:
            print(f"❌ Failed generating {file}: {e}")
            failed += 1
    
    print()
    print("=" * 100)
    print(f"✅ Generation complete: {generated} files, {failed} failed, {total_records} total records")


if __name__ == "__main__":
    generate()
