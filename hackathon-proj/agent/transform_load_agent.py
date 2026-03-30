import json
import os
import re
import time
from pathlib import Path
import ssl

import pandas as pd
import pyexasol

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR.parent / "data"
SYNTHETIC_DATA_PATH = BASE_DIR.parent / "synthetic_data"
MAPPING_OUTPUT_PATH = BASE_DIR / "mapping_output.json"
NUMERIC_COLS = [
    "planned_time_min",
    "run_time_min",
    "downtime_min",
    "total_units",
    "good_units",
    "defective_units",
    "availability",
    "performance",
    "quality",
    "oee_normalized",
]
FINAL_COLS = [
    "plant_id",
    "date",
    "planned_time_min",
    "run_time_min",
    "downtime_min",
    "total_units",
    "good_units",
    "defective_units",
    "availability",
    "performance",
    "quality",
    "oee_normalized",
    "data_source_type",
]
MES_TYPES = ["event", "kpi", "production", "time", "quality"]


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


def _extract_dsn_from_logs() -> str | None:
    pattern = re.compile(r'([A-Za-z0-9._-]+/[0-9a-f]{64}:[0-9]+)')
    log_dir = Path.home() / ".exanano" / "logs"
    if not log_dir.exists():
        return None

    files = sorted(
        (p for p in log_dir.iterdir() if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    for file_path in files[:80]:
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        matches = pattern.findall(text)
        if matches:
            return matches[-1]
    return None


def _candidate_dsns() -> list[str]:
    candidates: list[str] = []
    env_dsn = os.getenv("EXASOL_DSN")
    log_dsn = _extract_dsn_from_logs()
    default_dsn = "127.0.0.1:8563"
    for dsn in (env_dsn, log_dsn, default_dsn):
        if dsn and dsn not in candidates:
            candidates.append(dsn)
    return candidates


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _target_schema() -> str:
    return os.getenv("EXASOL_SCHEMA", "HACKATHON")


def _target_table() -> str:
    return os.getenv("EXASOL_TABLE", "UNIFIED_KPI")


def _target_fq_table() -> str:
    return f"{_quote_ident(_target_schema())}.{_quote_ident(_target_table())}"


def _target_import_table() -> tuple[str, str]:
    return (_target_schema(), _target_table())


def _data_mode() -> str:
    mode = os.getenv("PIPELINE_DATA_MODE", "files").strip().lower()
    return mode if mode in {"files", "synthetic"} else "files"


def _data_source_path() -> Path:
    """Return the appropriate data source path based on mode."""
    mode = _data_mode()
    if mode == "synthetic":
        return SYNTHETIC_DATA_PATH
    return DATA_PATH


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _extract_plant_id(file_name: str) -> str:
    match = re.search(r"(P\d{2})", file_name, flags=re.IGNORECASE)
    if match:
        return match.group(1).upper()
    stem = Path(file_name).stem.upper()
    return stem[:10]


def _ensure_target_table(conn) -> None:
    schema = _quote_ident(_target_schema())
    table = _target_fq_table()
    conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(
        f"""
CREATE TABLE IF NOT EXISTS {table} (
    "plant_id" VARCHAR(100),
    "date" DATE,
    "planned_time_min" DOUBLE,
    "run_time_min" DOUBLE,
    "downtime_min" DOUBLE,
    "total_units" DOUBLE,
    "good_units" DOUBLE,
    "defective_units" DOUBLE,
    "availability" DOUBLE,
    "performance" DOUBLE,
    "quality" DOUBLE,
    "oee_normalized" DOUBLE,
    "data_source_type" VARCHAR(50)
)
"""
    )


# 🔌 Exasol Connection
def connect():
    _load_env_file()

    user = os.getenv("EXASOL_USER", "sys")
    password = os.getenv("EXASOL_PASSWORD", "exasol")
    dsns = _candidate_dsns()
    last_error = None

    for attempt in range(1, 13):
        for dsn in dsns:
            try:
                return pyexasol.connect(
                    dsn=dsn,
                    user=user,
                    password=password,
                    encryption=True,
                    websocket_sslopt={
                        "cert_reqs": ssl.CERT_NONE,
                        "check_hostname": False,
                        "ssl_version": ssl.PROTOCOL_TLS_CLIENT,
                    },
                    verbose_error=True,
                )
            except Exception as exc:
                last_error = exc

        if attempt < 12:
            print(
                f"⚠️ Exasol connection attempt {attempt}/12 failed, retrying in 2s..."
            )
            time.sleep(2)

    raise RuntimeError(f"Failed to connect to Exasol after retries: {last_error}")


# 🧠 Transformation Logic
def transform(df, schema):

    df.columns = [c.lower() for c in df.columns]

    try:
        if schema == "production":
            df["planned_time_min"] = df["planned_hours"] * 60
            df["run_time_min"] = df["run_hours"] * 60
            df["downtime_min"] = (df["planned_hours"] - df["run_hours"]) * 60

            df["total_units"] = df.get("units_produced", 0)
            df["defective_units"] = df.get("units_defective", 0)
            df["good_units"] = df["total_units"] - df["defective_units"]

            df["availability"] = df["run_hours"] / df["planned_hours"]
            df["performance"] = 1
            df["quality"] = df["good_units"] / df["total_units"]

        elif schema == "kpi":
            df["availability"] = df.get("availability_pct", 0) / 100
            df["performance"] = df.get("performance_pct", 0) / 100
            df["quality"] = df.get("quality_pct", 0) / 100

        elif schema == "event":
            df["planned_time_min"] = df.get("duration_sec", 0) / 60
            df["run_time_min"] = df.get("duration_sec", 0) / 60
            df["downtime_min"] = 0

        elif schema == "time":
            df["planned_time_min"] = df.get("total_hours", 0) * 60
            df["run_time_min"] = df.get("operating_hours", 0) * 60
            df["downtime_min"] = (df.get("total_hours", 0) - df.get("operating_hours", 0)) * 60

        elif schema == "quality":
            df["total_units"] = df.get("total_units", 0)
            df["defective_units"] = df.get("defective_units", 0)
            df["good_units"] = df["total_units"] - df["defective_units"]

            df["availability"] = 0.9
            df["performance"] = 1
            df["quality"] = df["good_units"] / df["total_units"]

        # OEE Calculation
        if all(col in df.columns for col in ["availability", "performance", "quality"]):
            df["oee_normalized"] = (
                df["availability"] *
                df["performance"] *
                df["quality"]
            )

    except Exception as e:
        print(f"⚠️ Transformation issue: {e}")

    return df


def load_dataframe(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path, engine="openpyxl")
    if suffix == ".jsonl":
        # Read JSONL (JSON Lines) format - one JSON object per line
        records = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        return pd.DataFrame(records)
    if suffix == ".json":
        return pd.read_json(file_path)
    raise ValueError(f"Unsupported file type: {file_path.suffix}")


# 🚀 Main Pipeline
def run():
    conn = connect()
    _ensure_target_table(conn)
    target_table = _target_fq_table()
    target_import_table = _target_import_table()
    data_mode = _data_mode()
    data_source_path = _data_source_path()

    with MAPPING_OUTPUT_PATH.open(encoding="utf-8") as f:
        mapping_data = json.load(f)

    loaded = 0
    failed = 0

    if data_mode == "synthetic":
        print(f"📁 Synthetic mode: Reading from {SYNTHETIC_DATA_PATH}")
    else:
        print(f"📁 File mode: Reading from {DATA_PATH}")

    for item in mapping_data:
        file = item.get("file", "")
        schema = str(item.get("schema", "unknown")).strip().lower()

        if schema not in MES_TYPES:
            print(f"⚠️ Skipping {file or '<unknown file>'}: unsupported schema '{schema}'")
            failed += 1
            continue

        # In synthetic mode, convert .xlsx to .jsonl
        if data_mode == "synthetic":
            file_to_load = file.replace(".xlsx", ".jsonl").replace(".xls", ".jsonl")
        else:
            file_to_load = file

        file_path = data_source_path / file_to_load
        print(f"\n🚀 Processing {file} ({schema})")

        if not file_path.exists():
            print(f"⚠️ Skipping {file}: not found in {data_source_path}")
            failed += 1
            continue

        # Read file safely
        try:
            df = load_dataframe(file_path)
        except Exception as e:
            print(f"⚠️ Skipping {file}: {e}")
            failed += 1
            continue

        # Transform
        df = transform(df, schema)

        # Add metadata
        df["data_source_type"] = schema

        # 🚨 VALIDATIONS
        if df.empty:
            print(f"⚠️ Skipping {file} (empty dataframe)")
            failed += 1
            continue

        if "plant_id" not in df.columns:
            print(f"⚠️ Skipping {file} (missing plant_id)")
            failed += 1
            continue

        if "date" not in df.columns:
            print(f"⚠️ Skipping {file} (missing date)")
            failed += 1
            continue

        # Normalize columns to stable import shape
        for col in FINAL_COLS:
            if col not in df.columns:
                df[col] = None
        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df = df[FINAL_COLS]

        # Debug (optional)
        print("Columns:", df.columns.tolist())
        print("Rows:", len(df))

        # 🚀 Load into Exasol
        try:
            conn.import_from_pandas(df, target_import_table)
            print(f"✅ Loaded {file}")
            loaded += 1
        except Exception as e:
            print(f"❌ Failed loading {file}: {e}")
            failed += 1

    print(f"\n✅ Load finished: loaded={loaded}, failed={failed}, target={target_table}")


if __name__ == "__main__":
    run()
