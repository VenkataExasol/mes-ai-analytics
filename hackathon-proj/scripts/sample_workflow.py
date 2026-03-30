#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exasol_interface import ExasolClient, ExasolConfig, MESConnector, TableFileStore


def main() -> None:
    config = ExasolConfig.from_env()
    db = ExasolClient(config)
    file_store = TableFileStore(db)
    connector = MESConnector(db_client=db, file_store=file_store)

    normalized_json_path = ROOT / "scripts" / "sample_normalized.json"
    raw_json_path = ROOT / "scripts" / "sample_raw_rows.json"

    normalized_from_file = json.loads(normalized_json_path.read_text(encoding="utf-8"))
    raw_from_file = json.loads(raw_json_path.read_text(encoding="utf-8"))

    normalized_from_python = [
        {
            "plant_name": "Plant 2",
            "date": "2026-03-26",
            "planned_time_min": 480,
            "run_time_min": 450,
            "downtime_min": 30,
            "total_units": 12000,
            "good_units": 11700,
            "rejected_units": 300,
            "availability": 0.9375,
            "performance": 0.91,
            "quality": 0.975,
            "data_source_type": "kpi-based",
            "oee_normalized": 0.832,
        }
    ]

    with db:
        connector.ensure_default_tables()

        n1 = connector.insert_normalized_rows(normalized_from_file)
        n2 = connector.insert_normalized_rows(normalized_from_python)
        r1 = connector.insert_raw_rows(raw_from_file)
        print(f"Inserted normalized rows: {n1 + n2}, raw rows: {r1}")

        selected = connector.query_selected_rows(
            table="NORMALIZED_UNIFIED",
            columns=["plant_name", "date", "data_source_type", "oee_normalized"],
            where_sql='"plant_name" IN (\'Plant 01\', \'Plant 2\')',
            order_by_sql='"date" DESC',
            limit=10,
        )
        print("Selected normalized rows:")
        for row in selected:
            print(row)

        file_record = connector.store_raw_file(
            local_path=ROOT / "data" / "plant_P01.xlsx",
            remote_path="plants/plant_P01.xlsx",
            plant_id="P01",
        )
        print("Stored file:", file_record)

        file_rows = connector.list_raw_files(prefix="plants", limit=10)
        print("Stored file records:")
        for row in file_rows:
            print(row)


if __name__ == "__main__":
    main()
