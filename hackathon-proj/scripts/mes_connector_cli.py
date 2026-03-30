#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exasol_interface import (
    ExasolClient,
    ExasolConfig,
    MESConnector,
    TableFileStore,
    TableFileStoreConfig,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Table-based Exasol connector for normalized data, raw data, and raw files."
    )
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--objects-table", default="RAW_FILES_OBJECTS")
    parser.add_argument("--chunks-table", default="RAW_FILES_CHUNKS")
    parser.add_argument(
        "--normalizer-factory",
        default=None,
        help="Optional plugin factory path: module.submodule:factory_or_class",
    )
    parser.add_argument(
        "--sql-generator-factory",
        default=None,
        help="Optional plugin factory path: module.submodule:factory_or_class",
    )
    parser.add_argument(
        "--result-parser-factory",
        default=None,
        help="Optional plugin factory path: module.submodule:factory_or_class",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Initialize schema and default tables.")

    schema_create = subparsers.add_parser("schema-create", help="Create schema dynamically.")
    schema_create.add_argument("--name", required=True, help="Schema name")

    table_create = subparsers.add_parser("table-create", help="Create table with explicit column types.")
    table_create.add_argument("--table", required=True)
    table_create.add_argument(
        "--columns",
        required=True,
        help='Column spec: "col1:VARCHAR(100),col2:DECIMAL(18,2),col3:DATE"',
    )
    table_create.add_argument("--target-schema", default=None)
    table_create.add_argument("--add-ingested-at", action="store_true")

    table_ensure = subparsers.add_parser(
        "table-ensure",
        help="Create/extend table dynamically from sample row JSON keys.",
    )
    table_ensure.add_argument("--table", required=True)
    table_ensure.add_argument("--file", required=True)
    table_ensure.add_argument("--target-schema", default=None)

    norm_insert = subparsers.add_parser("normalized-insert", help="Insert normalized rows from JSON.")
    norm_insert.add_argument("--file", required=True)
    norm_insert.add_argument("--table", default="NORMALIZED_UNIFIED")

    norm_query = subparsers.add_parser("normalized-query", help="Query normalized rows.")
    _add_query_args(norm_query, default_table="NORMALIZED_UNIFIED")

    norm_remove = subparsers.add_parser("normalized-remove", help="Remove normalized rows by WHERE.")
    _add_remove_args(norm_remove, default_table="NORMALIZED_UNIFIED")

    raw_insert = subparsers.add_parser("raw-insert", help="Insert raw data rows from JSON.")
    raw_insert.add_argument("--file", required=True)
    raw_insert.add_argument("--table", default="RAW_DATA_WIDE")

    raw_query = subparsers.add_parser("raw-query", help="Query raw data rows.")
    _add_query_args(raw_query, default_table="RAW_DATA_WIDE")

    raw_remove = subparsers.add_parser("raw-remove", help="Remove raw data rows by WHERE.")
    _add_remove_args(raw_remove, default_table="RAW_DATA_WIDE")

    any_insert = subparsers.add_parser("table-insert", help="Insert rows into any table from JSON.")
    any_insert.add_argument("--table", required=True)
    any_insert.add_argument("--file", required=True)

    any_query = subparsers.add_parser("table-query", help="Query selected rows from any table.")
    _add_query_args(any_query, default_table=None, table_required=True)

    any_remove = subparsers.add_parser("table-remove", help="Remove rows from any table by WHERE.")
    _add_remove_args(any_remove, default_table=None, table_required=True)

    upload_file = subparsers.add_parser("file-upload", help="Upload raw file to table store.")
    upload_file.add_argument("--path", required=True)
    upload_file.add_argument("--remote-path", default=None)
    upload_file.add_argument("--plant-id", default=None)

    upload_dir = subparsers.add_parser("file-upload-dir", help="Upload directory files to table store.")
    upload_dir.add_argument("--path", required=True)
    upload_dir.add_argument("--remote-prefix", default="")

    file_list = subparsers.add_parser("file-list", help="List stored raw files.")
    file_list.add_argument("--prefix", default=None)
    file_list.add_argument("--limit", type=int, default=500)

    file_download = subparsers.add_parser("file-download", help="Download file by remote path.")
    file_download.add_argument("--remote-path", required=True)
    file_download.add_argument("--out", required=True)

    file_remove = subparsers.add_parser("file-remove", help="Delete stored file by remote path.")
    file_remove.add_argument("--remote-path", required=True)

    custom_query = subparsers.add_parser("query", help="Run custom SQL.")
    custom_query.add_argument("--sql", required=True)

    args = parser.parse_args()

    db_client = ExasolClient(_resolve_db_config(args))
    file_store = TableFileStore(
        db_client,
        config=TableFileStoreConfig(
            objects_table=args.objects_table,
            chunks_table=args.chunks_table,
        ),
    )
    connector = MESConnector(
        db_client=db_client,
        file_store=file_store,
        normalizer=_load_plugin(args.normalizer_factory),
        sql_generator=_load_plugin(args.sql_generator_factory),
        result_parser=_load_plugin(args.result_parser_factory),
    )

    with db_client:
        if args.command == "init":
            connector.ensure_default_tables()
            print("Initialized connector tables")
            return

        if args.command == "schema-create":
            connector.create_schema(args.name, if_not_exists=True)
            print(f"Schema ensured: {args.name}")
            return

        if args.command == "table-create":
            connector.create_table(
                table=args.table,
                columns=_parse_column_specs(args.columns),
                schema=args.target_schema,
                if_not_exists=True,
                add_ingested_at=args.add_ingested_at,
            )
            print(f"Table ensured: {args.target_schema or db_client.config.schema}.{args.table}")
            return

        if args.command == "table-ensure":
            rows = _load_rows(Path(args.file))
            connector.ensure_dynamic_table(
                table=args.table,
                sample_rows=rows,
                schema=args.target_schema,
            )
            print(
                f"Table dynamically ensured: {args.target_schema or db_client.config.schema}.{args.table}"
            )
            return

        if args.command == "normalized-insert":
            count = connector.insert_normalized_rows(_load_rows(Path(args.file)), table=args.table)
            print(f"Inserted {count} row(s) into {args.table}")
            return

        if args.command == "normalized-query":
            _print_rows(
                connector.query_normalized_rows(
                    table=args.table,
                    columns=_parse_columns(args.columns),
                    where_sql=args.where,
                    order_by_sql=args.order_by,
                    limit=args.limit,
                )
            )
            return

        if args.command == "normalized-remove":
            removed = connector.remove_normalized_rows(where_sql=args.where, table=args.table)
            print(f"Removed {removed} row(s) from {args.table}")
            return

        if args.command == "raw-insert":
            count = connector.insert_raw_rows(_load_rows(Path(args.file)), table=args.table)
            print(f"Inserted {count} row(s) into {args.table}")
            return

        if args.command == "raw-query":
            _print_rows(
                connector.query_raw_rows(
                    table=args.table,
                    columns=_parse_columns(args.columns),
                    where_sql=args.where,
                    order_by_sql=args.order_by,
                    limit=args.limit,
                )
            )
            return

        if args.command == "raw-remove":
            removed = connector.remove_raw_rows(where_sql=args.where, table=args.table)
            print(f"Removed {removed} row(s) from {args.table}")
            return

        if args.command == "table-insert":
            count = connector.insert_rows_to_table(table=args.table, rows=_load_rows(Path(args.file)))
            print(f"Inserted {count} row(s) into {args.table}")
            return

        if args.command == "table-query":
            rows = connector.query_selected_rows(
                table=args.table,
                columns=_parse_columns(args.columns),
                where_sql=args.where,
                order_by_sql=args.order_by,
                limit=args.limit,
            )
            _print_rows(rows)
            return

        if args.command == "table-remove":
            removed = connector.remove_rows_from_table(table=args.table, where_sql=args.where)
            print(f"Removed {removed} row(s) from {args.table}")
            return

        if args.command == "file-upload":
            record = connector.store_raw_file(
                local_path=args.path,
                remote_path=args.remote_path,
                plant_id=args.plant_id,
            )
            print(record)
            return

        if args.command == "file-upload-dir":
            records = connector.store_raw_directory(directory=args.path, remote_prefix=args.remote_prefix)
            print(f"Uploaded {len(records)} file(s)")
            for record in records:
                print(record)
            return

        if args.command == "file-list":
            rows = connector.list_raw_files(prefix=args.prefix, recursive=True, limit=args.limit)
            _print_rows(rows)
            return

        if args.command == "file-download":
            out = connector.download_raw_file(remote_path=args.remote_path, local_path=args.out)
            print(f"Downloaded to {out}")
            return

        if args.command == "file-remove":
            removed = connector.remove_raw_file(remote_path=args.remote_path)
            print(f"Removed {removed} file record(s)")
            return

        if args.command == "query":
            result = connector.run_query({"sql": args.sql})
            if isinstance(result, list):
                _print_rows(result)
            else:
                print(result)


def _resolve_db_config(args: argparse.Namespace) -> ExasolConfig:
    if args.dsn and args.user and args.password:
        return ExasolConfig(
            dsn=args.dsn,
            user=args.user,
            password=args.password,
            schema=args.schema or "HACKATHON",
        )
    env_cfg = ExasolConfig.from_env()
    return ExasolConfig(
        dsn=env_cfg.dsn,
        user=env_cfg.user,
        password=env_cfg.password,
        schema=args.schema or env_cfg.schema,
        table=env_cfg.table,
    )


def _load_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("records"), list):
        return payload["records"]
    raise ValueError("JSON must be list[dict] or {'records': list[dict]}")


def _parse_columns(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    cols = [c.strip() for c in raw.split(",") if c.strip()]
    return cols or None


def _parse_column_specs(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    parts = [p.strip() for p in _split_top_level(raw) if p.strip()]
    for part in parts:
        if ":" not in part:
            raise ValueError(
                f"Invalid column spec '{part}'. Expected format name:TYPE, e.g. id:VARCHAR(100)"
            )
        name, sql_type = part.split(":", 1)
        name = name.strip()
        sql_type = sql_type.strip()
        if not name or not sql_type:
            raise ValueError(f"Invalid column spec '{part}'")
        out[name] = sql_type
    if not out:
        raise ValueError("At least one column definition is required")
    return out


def _split_top_level(raw: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in raw:
        if ch == "(":
            depth += 1
            buf.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
            continue
        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if buf:
        parts.append("".join(buf))
    return parts


def _add_query_args(
    parser: argparse.ArgumentParser,
    default_table: str | None,
    table_required: bool = False,
) -> None:
    if table_required:
        parser.add_argument("--table", required=True)
    else:
        parser.add_argument("--table", default=default_table)
    parser.add_argument("--columns", default=None, help="Comma-separated columns")
    parser.add_argument("--where", default=None, help='SQL predicate, e.g. "\"plant_name\" = \'Plant 01\'"')
    parser.add_argument("--order-by", default=None, help='SQL ORDER BY body, e.g. "\"date\" DESC"')
    parser.add_argument("--limit", type=int, default=1000)


def _add_remove_args(
    parser: argparse.ArgumentParser,
    default_table: str | None,
    table_required: bool = False,
) -> None:
    if table_required:
        parser.add_argument("--table", required=True)
    else:
        parser.add_argument("--table", default=default_table)
    parser.add_argument("--where", required=True, help='SQL predicate, e.g. "\"plant_name\" = \'Plant 01\'"')


def _print_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        print(row)
    print(f"Returned {len(rows)} row(s)")


def _load_plugin(factory_path: str | None) -> Any:
    if not factory_path:
        return None
    if ":" not in factory_path:
        raise ValueError("Plugin path must be in format module.submodule:factory_or_class")
    module_name, symbol_name = factory_path.split(":", 1)
    module = importlib.import_module(module_name)
    symbol = getattr(module, symbol_name)
    return symbol() if callable(symbol) else symbol


if __name__ == "__main__":
    main()
