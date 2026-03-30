import os
import re
import ssl
import time
import csv
import json
import ast
import difflib
import datetime as dt
import threading
from pathlib import Path

import altair as alt
import pandas as pd
import pyexasol
import requests
import streamlit as st

# Near real-time analytics modules
from change_detector import check_data_changed, get_table_row_count, get_table_max_timestamp
from async_query_worker import get_worker, invalidate_cache

COLUMN_CANDIDATES = {
    "plant_id": ["plant_id"],
    "plant_name": ["plant_name"],
    "country": ["country"],
    "date": ["date"],
    "shift": ["shift"],
    "planned_time_min": ["planned_time_min"],
    "run_time_min": ["run_time_min"],
    "downtime_min": ["downtime_min"],
    "total_units": ["total_units"],
    "good_units": ["good_units"],
    "defective_units": ["defective_units", "rejected_units"],
    "availability": ["availability"],
    "performance": ["performance"],
    "quality": ["quality"],
    "oee_reported": ["oee_reported", "oee"],
    "oee_normalized": ["oee_normalized"],
    "data_source_type": ["data_source_type"],
}

FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE|MERGE|CALL|EXEC|EXECUTE|IMPORT|EXPORT)\b",
    flags=re.IGNORECASE,
)

SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "HAVING",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "ON", "AND", "OR",
    "ASC", "DESC", "NULLS", "LAST", "FIRST", "UNION", "WITH", "AS", "DISTINCT",
    "CASE", "WHEN", "THEN", "ELSE", "END", "IN", "IS", "NOT", "LIKE", "BETWEEN",
    "OVER", "PARTITION", "ROWS", "RANGE", "CURRENT", "ROW", "UNBOUNDED",
}

AI_CHART_TYPES = {
    "auto",
    "line",
    "bar",
    "bar_sorted",
    "stacked_bar",
    "pie",
    "scatter",
    "heatmap",
    "hist",
}


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _extract_dsn_from_logs() -> str | None:
    pattern = re.compile(r"([A-Za-z0-9._-]+/[0-9a-f]{64}:[0-9]+)")
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


def _escape_sql_str(value: str) -> str:
    return value.replace("'", "''")


def _extract_columns(stmt) -> list[str]:
    meta = stmt.columns()
    if not meta:
        return []
    if isinstance(meta, dict):
        return list(meta.keys())
    if isinstance(meta[0], dict):
        return [str(col.get("name", "")) for col in meta]
    return [str(col) for col in meta]


def _connect() -> pyexasol.ExaConnection:
    _load_env_file()
    user = os.getenv("EXASOL_USER", "sys")
    password = os.getenv("EXASOL_PASSWORD", "exasol")
    dsns = _candidate_dsns()
    last_error = None

    for _attempt in range(1, 9):
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
            except Exception as exc:  # pragma: no cover - runtime connectivity path
                last_error = exc
        time.sleep(1)

    raise RuntimeError(f"Failed to connect to Exasol: {last_error}")


def _discover_table(conn, schema: str, table: str) -> tuple[str, str, list[str]]:
    safe_schema = _escape_sql_str(schema)
    safe_table = _escape_sql_str(table)
    sql = f"""
SELECT "COLUMN_SCHEMA", "COLUMN_TABLE", "COLUMN_NAME"
FROM "EXA_ALL_COLUMNS"
WHERE UPPER("COLUMN_SCHEMA") = UPPER('{safe_schema}')
  AND UPPER("COLUMN_TABLE") = UPPER('{safe_table}')
ORDER BY "COLUMN_ORDINAL_POSITION"
"""
    stmt = conn.execute(sql)
    rows = stmt.fetchall()
    if not rows:
        raise ValueError(
            f"Table not found or has no columns: schema={schema}, table={table}"
        )

    actual_schema = str(rows[0][0])
    actual_table = str(rows[0][1])
    columns = [str(row[2]) for row in rows]
    return actual_schema, actual_table, columns


def _build_column_mapping(available_columns: list[str]) -> dict[str, str]:
    lower_map = {col.lower(): col for col in available_columns}
    mapping: dict[str, str] = {}

    for canonical, candidates in COLUMN_CANDIDATES.items():
        for candidate in candidates:
            if candidate.lower() in lower_map:
                mapping[canonical] = lower_map[candidate.lower()]
                break

    return mapping


def _canonical_column_rename_map(columns: list[str]) -> dict[str, str]:
    lower_to_actual = {c.lower(): c for c in columns}
    rename: dict[str, str] = {}
    for canonical, candidates in COLUMN_CANDIDATES.items():
        if canonical.lower() in lower_to_actual:
            continue
        for candidate in candidates:
            actual = lower_to_actual.get(candidate.lower())
            if actual and actual != canonical:
                rename[actual] = canonical
                break
    return rename


def _target_fq_table(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _normalize_sql(sql: str) -> str:
    cleaned = sql.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()

    tokens = cleaned.split()
    if tokens:
        last = tokens[-1].upper()
        sql_keywords = {
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "HAVING",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AND", "OR",
            "ASC", "DESC", "NULLS", "LAST", "FIRST", "UNION", "WITH",
        }
        if (
            last.isalpha()
            and 3 <= len(last) < 8
            and last not in sql_keywords
            and any(keyword.startswith(last) for keyword in sql_keywords)
        ):
            cleaned = " ".join(tokens[:-1]).strip()
    return cleaned


def _contains_multiple_statements(sql: str) -> bool:
    return ";" in sql


def _ensure_safe_readonly_sql(sql: str, table: str) -> tuple[bool, str]:
    normalized = _normalize_sql(sql)
    lower_sql = normalized.lower()

    if not normalized:
        return False, "SQL is empty."
    if _contains_multiple_statements(normalized):
        return False, "Multiple SQL statements are not allowed."
    if not (lower_sql.startswith("select ") or lower_sql.startswith("with ")):
        return False, "Only SELECT / WITH queries are allowed."
    if FORBIDDEN_SQL.search(normalized):
        return False, "Only read-only SQL is allowed."
    if table.lower() not in lower_sql:
        return False, "Query must target the configured unified table."
    return True, normalized


def _append_limit(sql: str, row_limit: int = 500) -> str:
    normalized = _normalize_sql(sql)
    match = re.search(r"\blimit\s+(\d+)\b", normalized, flags=re.IGNORECASE)
    if not match:
        return f"{normalized}\nLIMIT {int(row_limit)}"

    existing = int(match.group(1))
    if existing <= row_limit:
        return normalized
    return re.sub(
        r"\blimit\s+\d+\b",
        f"LIMIT {int(row_limit)}",
        normalized,
        flags=re.IGNORECASE,
    )


def _run_query(sql: str) -> pd.DataFrame:
    conn = _connect()
    try:
        stmt = conn.execute(sql)
        rows = stmt.fetchall()
        columns = _extract_columns(stmt)
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def _write_audit_log(
    question: str,
    sql: str,
    status: str,
    rows: int = 0,
    error: str = "",
) -> None:
    log_path = Path(__file__).resolve().parent / "ask_data_audit.csv"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_path.exists()
    with log_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(
                ["timestamp", "question", "sql", "status", "rows", "error"]
            )
        writer.writerow(
            [dt.datetime.utcnow().isoformat(), question, sql, status, rows, error]
        )


def _strip_sql_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:sql)?", "", cleaned, flags=re.IGNORECASE).strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()
    return cleaned


def _extract_first_sql_query(text: str) -> str:
    cleaned = _strip_sql_fences(text).strip()
    if not cleaned:
        return cleaned

    cleaned = re.sub(r"(?is)^.*?\b(sqlquery|sql|answer)\s*:\s*", "", cleaned).strip()

    match = re.search(r"(?is)\b(with|select)\b", cleaned)
    if match:
        cleaned = cleaned[match.start():]

    cleaned = re.split(r"(?im)^\s*(explanation|notes?)\s*:\s*", cleaned, maxsplit=1)[0].strip()

    if ";" in cleaned:
        cleaned = cleaned.split(";", 1)[0]
    return cleaned.strip()


def _enforce_target_table(sql: str, schema: str, table: str) -> str:
    target = _target_fq_table(schema, table)
    table_ref_pattern = re.compile(
        r'(?is)\b(from|join)\s+((?:(?:"[^"]+"|\w+)\s*\.\s*)?(?:"[^"]+"|\w+))'
    )
    return table_ref_pattern.sub(lambda m: f"{m.group(1)} {target}", sql)


def _strip_target_table_column_qualifiers(sql: str, schema: str, table: str) -> str:
    schema_pat = re.escape(schema)
    table_pat = re.escape(table)
    out = sql

    # "SCHEMA"."TABLE"."col" -> "col"
    out = re.sub(
        rf'(?is)"{schema_pat}"\s*\.\s*"{table_pat}"\s*\.\s*"([^"]+)"',
        lambda m: f'"{m.group(1)}"',
        out,
    )
    # SCHEMA.TABLE.col -> "col"
    out = re.sub(
        rf'(?i)\b{schema_pat}\s*\.\s*{table_pat}\s*\.\s*([A-Za-z_][A-Za-z0-9_$]*)\b',
        lambda m: f'"{m.group(1)}"',
        out,
    )
    # "TABLE"."col" -> "col"
    out = re.sub(
        rf'(?is)"{table_pat}"\s*\.\s*"([^"]+)"',
        lambda m: f'"{m.group(1)}"',
        out,
    )
    # TABLE.col -> "col"
    out = re.sub(
        rf'(?i)\b{table_pat}\s*\.\s*([A-Za-z_][A-Za-z0-9_$]*)\b',
        lambda m: f'"{m.group(1)}"',
        out,
    )
    return out


def _correct_column_identifiers(sql: str, available_columns: list[str]) -> str:
    if not available_columns:
        return sql

    allowed_lower_to_actual = {c.lower(): c for c in available_columns}

    def _replace_identifier(match: re.Match) -> str:
        ident = match.group(1)
        ident_lower = ident.lower()

        if ident_lower in allowed_lower_to_actual:
            return f'"{allowed_lower_to_actual[ident_lower]}"'

        if ident_lower.startswith(("avg_", "sum_", "count_", "min_", "max_")):
            return match.group(0)

        close = difflib.get_close_matches(
            ident_lower,
            list(allowed_lower_to_actual.keys()),
            n=1,
            cutoff=0.78,
        )
        if close:
            return f'"{allowed_lower_to_actual[close[0]]}"'
        return match.group(0)

    # Do not rewrite quoted schema / table identifiers used as "schema"."table"
    return re.sub(r'(?<!\.)"([^"]+)"(?!\s*\.)', _replace_identifier, sql)


def _correct_bare_column_identifiers(sql: str, available_columns: list[str]) -> str:
    if not available_columns:
        return sql

    allowed_lower_to_actual = {c.lower(): c for c in available_columns}

    def _replace_bare(match: re.Match) -> str:
        token = match.group(1)
        lower = token.lower()
        upper = token.upper()

        if upper in SQL_KEYWORDS or lower in {"null", "true", "false"}:
            return token

        if lower in allowed_lower_to_actual:
            return f'"{allowed_lower_to_actual[lower]}"'

        close = difflib.get_close_matches(
            lower,
            list(allowed_lower_to_actual.keys()),
            n=1,
            cutoff=0.86,
        )
        if close:
            return f'"{allowed_lower_to_actual[close[0]]}"'

        return token

    # Match unquoted identifiers only, skip dotted refs and already-quoted pieces.
    return re.sub(r'(?<![".\w])([A-Za-z_][A-Za-z0-9_$]*)(?!["\w])', _replace_bare, sql)


def _repair_semantic_column_aliases(sql: str, available_columns: list[str]) -> str:
    lower_to_actual = {c.lower(): c for c in available_columns}
    out = sql

    replacements: list[tuple[str, str]] = []
    if "plant_id" not in lower_to_actual and "plant_name" in lower_to_actual:
        replacements.append(("plant_id", lower_to_actual["plant_name"]))
    if "defective_units" not in lower_to_actual and "rejected_units" in lower_to_actual:
        replacements.append(("defective_units", lower_to_actual["rejected_units"]))
    if "rejected_units" not in lower_to_actual and "defective_units" in lower_to_actual:
        replacements.append(("rejected_units", lower_to_actual["defective_units"]))

    for src, dst_actual in replacements:
        out = re.sub(rf'(?i)"{re.escape(src)}"', f'"{dst_actual}"', out)
        out = re.sub(rf'(?i)\b{re.escape(src)}\b', f'"{dst_actual}"', out)

    return out


def _postprocess_generated_sql(
    sql: str,
    schema: str,
    table: str,
    available_columns: list[str],
) -> str:
    out = _normalize_sql(sql)
    out = _enforce_target_table(out, schema=schema, table=table)
    out = _strip_target_table_column_qualifiers(out, schema=schema, table=table)
    out = re.sub(r"\bILIKE\b", "LIKE", out, flags=re.IGNORECASE)
    out = _repair_semantic_column_aliases(out, available_columns=available_columns)
    out = _correct_column_identifiers(out, available_columns=available_columns)
    out = _correct_bare_column_identifiers(out, available_columns=available_columns)
    return _normalize_sql(out)


def _validate_exasol_sql(sql: str) -> tuple[bool, str]:
    """Validate SQL for Exasol compatibility. Returns (is_valid, error_message)."""
    normalized = _normalize_sql(sql).upper()
    
    # Check forbidden patterns
    forbidden_patterns = [
        (r"\bTRUNC\(", "TRUNC() not supported in Exasol; use SUBSTR() or CAST(... AS DATE)"),
        (r"\bDROP\s", "DROP statements not allowed"),
        (r"\bDELETE\b", "DELETE statements not allowed"),
        (r"\bINSERT\b", "INSERT statements not allowed"),
        (r"\bUPDATE\b", "UPDATE statements not allowed"),
        (r"\bCREATE\s", "CREATE statements not allowed"),
        (r"\bALTER\s", "ALTER statements not allowed"),
        (r"\bROW_NUMBER\(\)\s*OVER\s*\(\s*\)", "ROW_NUMBER() requires OVER clause with ORDER BY"),
    ]
    
    for pattern, msg in forbidden_patterns:
        if re.search(pattern, normalized, re.IGNORECASE):
            return False, msg
    
    # Check for GROUP BY when using aggregates (but ignore window functions with OVER)
    has_aggregate = bool(re.search(
        r"\b(COUNT|SUM|AVG|MIN|MAX|STDDEV|VARIANCE)\s*\(",
        normalized,
        re.IGNORECASE
    ))
    has_window_func = bool(re.search(r"\bOVER\s*\(", normalized, re.IGNORECASE))
    has_group_by = bool(re.search(r"\bGROUP\s+BY\b", normalized, re.IGNORECASE))
    
    # Allow pure aggregates without GROUP BY (e.g., "SELECT COUNT(*) FROM table")
    if has_aggregate and not has_window_func:
        # Check if the aggregate is the only thing selected (no non-aggregated columns)
        select_part_match = re.search(r"SELECT\s+(.+?)\s+FROM", normalized, re.IGNORECASE)
        if select_part_match:
            select_part = select_part_match.group(1)
            # If there are non-function columns (likely indicates a join or column without aggregate)
            if re.search(r'"[^"]+"\s*(?:,|\s+WHERE|\s+GROUP|\s+ORDER|FROM)', select_part) and not has_group_by:
                return False, "Query has aggregates and non-aggregated columns without GROUP BY"
    
    return True, ""


def _fix_exasol_sql_functions(sql: str) -> str:
    """Fix common SQL functions for Exasol compatibility."""
    out = sql
    
    # ILIKE → LIKE (Exasol doesn't have ILIKE, but LIKE is case-insensitive for VARCHAR)
    out = re.sub(r"\bILIKE\b", "LIKE", out, flags=re.IGNORECASE)
    
    # STRPOS → POSITION (Exasol uses POSITION for string search)
    out = re.sub(r"\bSTRPOS\s*\(", "POSITION(", out, flags=re.IGNORECASE)
    
    # SUBSTRING → SUBSTR (Exasol prefers SUBSTR)
    out = re.sub(r"\bSUBSTRING\s*\(", "SUBSTR(", out, flags=re.IGNORECASE)
    
    # NOW() → CURRENT_TIMESTAMP (Exasol uses CURRENT_TIMESTAMP for current time)
    out = re.sub(r"\bNOW\s*\(\)", "CURRENT_TIMESTAMP", out, flags=re.IGNORECASE)
    
    # CURRENT_DATE() → CURRENT_DATE (Exasol uses CURRENT_DATE without parens)
    out = re.sub(r"\bCURRENT_DATE\s*\(\)", "CURRENT_DATE", out, flags=re.IGNORECASE)
    
    # DATE_TRUNC → Use CAST(...AS DATE) for date part, or date arithmetic
    # Example: DATE_TRUNC('month', date_col) → CAST(date_col AS DATE)
    out = re.sub(
        r"\bDATE_TRUNC\s*\(\s*['\"]([a-z]+)['\"]\s*,\s*([^)]+)\s*\)",
        lambda m: f'CAST({m.group(2)} AS DATE)',
        out,
        flags=re.IGNORECASE
    )
    
    # COALESCE stays the same (Exasol supports it)
    # CAST stays the same (Exasol supports it)
    # COUNT(*) with NULL handling
    # NULL → NULL (Exasol uses NULL)
    
    # Ensure LIMIT has no commas (e.g., "LIMIT 1,10" → "LIMIT 10 OFFSET 1")
    def _fix_limit_offset(match):
        parts = match.group(1).split(',')
        if len(parts) == 2:
            offset, limit = parts[0].strip(), parts[1].strip()
            return f"LIMIT {limit} OFFSET {offset}"
        return match.group(0)
    
    out = re.sub(r"LIMIT\s+(\d+\s*,\s*\d+)\b", _fix_limit_offset, out, flags=re.IGNORECASE)
    
    return out


def _validate_and_fix_exasol_sql(
    sql: str,
    schema: str,
    table: str,
    available_columns: list[str],
) -> tuple[str, list[str]]:
    """Validate and fix SQL for Exasol. Returns (fixed_sql, warnings)."""
    warnings = []
    
    # Step 1: Basic postprocessing
    fixed_sql = _postprocess_generated_sql(sql, schema, table, available_columns)
    
    # Step 2: Function translation
    fixed_sql = _fix_exasol_sql_functions(fixed_sql)
    
    # Step 3: Validation
    is_valid, error_msg = _validate_exasol_sql(fixed_sql)
    if not is_valid:
        warnings.append(f"SQL Validation: {error_msg}")
    
    # Step 4: Final normalization
    fixed_sql = _normalize_sql(fixed_sql)
    
    return fixed_sql, warnings


def _normalize_ai_chart_type(value: str | None) -> str:
    if not value:
        return "auto"
    v = value.strip().lower()
    aliases = {
        "circular": "pie",
        "donut": "pie",
        "doughnut": "pie",
        "histogram": "hist",
        "linechart": "line",
        "barchart": "bar",
        "barsorted": "bar_sorted",
        "stackedbar": "stacked_bar",
    }
    mapped = aliases.get(v, v)
    return mapped if mapped in AI_CHART_TYPES else "auto"


def _build_ollama_json_plan_prompt(
    question: str,
    schema: str,
    table: str,
    available_columns: list[str],
) -> str:
    table_ref = _target_fq_table(schema, table)
    cols_text = ", ".join(sorted(available_columns))
    
    # Enhanced context about the manufacturing data
    column_descriptions = {
        "plant_id": "Plant identifier (P01-P12 typically)",
        "plant_name": "Plant name or location",
        "date": "Production date (yyyy-mm-dd)",
        "time": "Time period or hour (hh:mm:ss)",
        "planned_time_min": "Planned production time in minutes",
        "run_time_min": "Actual runtime in minutes",
        "downtime_min": "Machine downtime in minutes",
        "availability": "Availability metric (0-1, % uptime)",
        "performance": "Performance metric (0-1, % speed efficiency)",
        "quality": "Quality metric (0-1, % good units)",
        "oee_normalized": "Overall Equipment Effectiveness (normalized): Availability × Performance × Quality",
        "oee_reported": "OEE as reported by the MES (may differ from calculated)",
        "total_units": "Total units produced",
        "good_units": "Good units produced (without defects)",
        "defect_units": "Units with defects",
        "data_source_type": "Source of data: 'production', 'kpi', 'event', 'time', or 'quality'",
    }
    
    # Build dynamic column documentation
    relevant_cols = [col for col in available_columns if col in column_descriptions]
    col_doc = "\n".join([f"  - {col}: {column_descriptions[col]}" for col in relevant_cols])
    
    return (
        "You are an Exasol SQL and chart planner for manufacturing KPI analysis.\n"
        "DATABASE: Exasol (in-memory columnar database)\n"
        "Return JSON only. No markdown, no explanation.\n\n"
        f"TABLE: {table_ref} (Unified OEE & Manufacturing Metrics)\n"
        f"SCHEMA: {schema}\n\n"
        "AVAILABLE COLUMNS (with descriptions):\n"
        f"{col_doc}\n\n"
        "DATA CONTEXT:\n"
        "- This is manufacturing data from 12 plants (P01-P12)\n"
        "- Key KPI: OEE (Overall Equipment Effectiveness) = Availability × Performance × Quality\n"
        "- Normalized OEE is calculated: A×P×Q (values 0-1 or 0-100%)\n"
        "- Some plants report OEE differently, so normalization reveals gaps\n"
        "- Data comes from multiple sources (production events, KPI reports, quality logs)\n"
        "- Goal: Find patterns, gaps, and performance insights\n\n"
        "SQL RULES:\n"
        "- Database: Exasol SQL dialect\n"
        "- SELECT / WITH only (read-only)\n"
        f"- Query ONLY: {table_ref}\n"
        f"- Allowed columns: {cols_text}\n"
        "- Use QUOTED identifiers for columns (e.g., \"column_name\")\n"
        "- Use LIKE not ILIKE\n"
        "- GROUP BY when aggregating, ORDER BY for clarity\n"
        "- LIMIT 500 default (unless user specifies otherwise)\n\n"
        "CHART TYPE SELECTION (choose based on analysis intent):\n"
        "  - line: Time series trends (date on x-axis, KPI on y-axis)\n"
        "  - bar: Comparisons across plants/categories (discrete x, value on y)\n"
        "  - bar_sorted: Ranking (top/bottom performers, highest/lowest KPIs)\n"
        "  - stacked_bar: Composition (A/P/Q breakdown, source types, component analysis)\n"
        "  - pie: Part-to-whole (percentage distribution, composition)\n"
        "  - scatter: Correlation (2 continuous metrics relationship)\n"
        "  - heatmap: Data quality/completeness (plant×date matrix with filled values)\n"
        "  - hist: Distribution (variance, spread, range of values)\n"
        "  - auto: Let frontend decide\n\n"
        "CHART DECISION RULES:\n"
        "  IF question mentions: 'trend', 'over time', 'daily', 'history' → line\n"
        "  IF question mentions: 'compare', 'vs', 'between plants' → bar\n"
        "  IF question mentions: 'top', 'bottom', 'best', 'worst', 'rank' → bar_sorted\n"
        "  IF question mentions: 'gap', 'inconsist', 'diff', 'A/P/Q', 'breakdown' → bar or stacked_bar\n"
        "  IF question mentions: 'quality', 'complete', 'missing', 'data quality' → heatmap\n"
        "  IF question mentions: 'distribution', 'range', 'variance', 'spread' → hist\n"
        "  IF question mentions: 'correlation', 'relationship', 'vs metric' → scatter\n\n"
        "JSON OUTPUT FORMAT:\n"
        "{\n"
        '  "sql": "SELECT ... FROM ... WHERE ... ORDER BY ... LIMIT 500",\n'
        '  "chart_type": "line|bar|bar_sorted|stacked_bar|pie|scatter|heatmap|hist|auto",\n'
        '  "x_axis": "column_name_for_x_dimension_or_category",\n'
        '  "y_axis": "column_name_for_value_or_metric",\n'
        '  "color": "column_name_for_grouping_or_empty_string",\n'
        '  "title": "human_readable_chart_title_or_empty_string"\n'
        "}\n\n"
        f"BUSINESS QUESTION: {question}\n"
    )


def _build_ollama_sql_only_prompt(
    question: str,
    schema: str,
    table: str,
    available_columns: list[str],
) -> str:
    table_ref = _target_fq_table(schema, table)
    cols_text = ", ".join(sorted(available_columns))
    
    # Enhanced context
    column_descriptions = {
        "plant_id": "Plant identifier (P01-P12)",
        "plant_name": "Plant name/location",
        "date": "Production date (yyyy-mm-dd)",
        "time": "Time or hour (hh:mm:ss)",
        "planned_time_min": "Planned production time (minutes)",
        "run_time_min": "Actual runtime (minutes)",
        "downtime_min": "Machine downtime (minutes)",
        "availability": "Availability 0-1",
        "performance": "Performance 0-1",
        "quality": "Quality 0-1",
        "oee_normalized": "OEE = Availability × Performance × Quality",
        "oee_reported": "OEE as reported (may differ from calculated)",
        "total_units": "Total units produced",
        "good_units": "Good units (no defects)",
        "defect_units": "Defective units",
        "data_source_type": "Data source: production/kpi/event/time/quality",
    }
    
    relevant_cols = [col for col in available_columns if col in column_descriptions]
    col_doc = "\n".join([f"  {col}: {column_descriptions[col]}" for col in relevant_cols])
    
    return (
        "Generate ONE Exasol SQL SELECT query for manufacturing KPI analysis.\n"
        "DATABASE: Exasol (columnar, in-memory)\n"
        f"TABLE: {table_ref}\n"
        f"SCHEMA: {schema}\n\n"
        "AVAILABLE COLUMNS:\n"
        f"{col_doc}\n\n"
        "DATA: 12 plants (P01-P12), OEE metrics, production data\n"
        "KEY METRIC: OEE (Normalized) = Availability × Performance × Quality\n"
        "PURPOSE: Analyze manufacturing performance, find gaps, identify trends\n\n"
        "RULES:\n"
        "- Exasol SQL dialect ONLY\n"
        "- SELECT or WITH (read-only, no INSERT/UPDATE/DELETE)\n"
        f"- Query ONLY: {table_ref}\n"
        f"- Allowed columns: {cols_text}\n"
        "- QUOTE all column identifiers: \"column_name\"\n"
        "- Use LIKE not ILIKE\n"
        "- GROUP BY when aggregating (required with aggregates)\n"
        "- ORDER BY for meaningful ordering\n"
        "- LIMIT 500 (or user-specified limit)\n"
        "- Joins only if multiple related aggregates needed\n\n"
        "RETURN: Plain SQL query ONLY (no explanation)\n\n"
        f"QUESTION: {question}\n"
    )


def _extract_json_object(text: str) -> dict:
    cleaned = _strip_sql_fences(str(text)).strip()
    if not cleaned:
        raise RuntimeError("Model returned empty payload.")

    def _extract_braced_payload(value: str) -> str | None:
        start = value.find("{")
        if start < 0:
            return None
        depth = 0
        in_quote = False
        quote_char = ""
        escape = False
        for idx in range(start, len(value)):
            ch = value[idx]
            if in_quote:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == quote_char:
                    in_quote = False
            else:
                if ch in {'"', "'"}:
                    in_quote = True
                    quote_char = ch
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        return value[start: idx + 1]
        return None

    def _repair_json_like(value: str) -> str:
        out = value.strip()
        out = re.sub(r"(?is)^```(?:json)?\s*", "", out).strip()
        out = re.sub(r"(?is)\s*```$", "", out).strip()
        # Remove trailing commas before closing braces / brackets.
        out = re.sub(r",\s*([}\]])", r"\1", out)
        # Quote bare keys: {sql: "..."} -> {"sql": "..."}
        out = re.sub(
            r'([{,]\s*)([A-Za-z_][A-Za-z0-9_\-]*)\s*:',
            r'\1"\2":',
            out,
        )
        return out

    def _parse_dict_candidate(value: str) -> dict | None:
        for candidate in (value, _repair_json_like(value)):
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
            try:
                literal = ast.literal_eval(candidate)
                if isinstance(literal, dict):
                    return {str(k): literal[k] for k in literal}
            except Exception:
                pass
        return None

    candidates: list[str] = [cleaned]
    braced = _extract_braced_payload(cleaned)
    if braced and braced not in candidates:
        candidates.append(braced)

    match = re.search(r"(?s)\{.*\}", cleaned)
    if match:
        regex_candidate = match.group(0)
        if regex_candidate not in candidates:
            candidates.append(regex_candidate)

    for candidate in candidates:
        parsed = _parse_dict_candidate(candidate)
        if parsed is not None:
            return parsed

    # Heuristic key-value fallback (handles partially structured outputs).
    key_values: dict[str, str] = {}
    for key in ["sql", "query", "statement", "select_sql", "chart_type", "x_axis", "y_axis", "color", "title"]:
        m = re.search(rf"(?im)^\s*{re.escape(key)}\s*[:=]\s*(.+?)\s*$", cleaned)
        if m:
            key_values[key] = m.group(1).strip().strip('"').strip("'")
    if key_values:
        return key_values

    sql_guess = _extract_first_sql_query(cleaned)
    if sql_guess:
        return {"sql": sql_guess}

    raise RuntimeError("Could not parse structured JSON output from model.")


def _sanitize_extracted_sql(value: str) -> str:
    out = str(value or "").strip()
    if not out:
        return out
    out = re.split(
        r'(?i)\b(?:chart_type|x_axis|y_axis|color|title)\b\s*[:=]',
        out,
        maxsplit=1,
    )[0].strip()
    out = out.rstrip(",")
    while out and out[-1] in {"'", '"', "`", "}", "]"}:
        out = out[:-1].rstrip()
    return out


def _generate_ai_plan_via_ollama(
    question: str,
    schema: str,
    table: str,
    available_columns: list[str],
    model_override: str | None = None,
) -> tuple[dict[str, str], str]:
    model = (model_override or os.getenv("OLLAMA_MODEL", "qwen3-coder-next")).strip()
    base_url = os.getenv("OLLAMA_BASE_URL", "https://ollama.com").strip().rstrip("/")
    api_key = os.getenv("OLLAMA_API_KEY", "").strip()
    timeout_sec = float(os.getenv("OLLAMA_TIMEOUT_SEC", "45"))
    num_predict = int(os.getenv("OLLAMA_NUM_PREDICT", "220"))
    temperature = float(os.getenv("OLLAMA_TEMPERATURE", "0"))
    seed = int(os.getenv("OLLAMA_SEED", "42"))
    keep_alive = os.getenv("OLLAMA_KEEP_ALIVE", "10m").strip()
    
    # Test connectivity first
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    
    try:
        test_response = requests.get(f"{base_url}/api/tags", headers=headers, timeout=5)
        if test_response.status_code >= 400:
            raise RuntimeError(f"Ollama service not responding at {base_url} (HTTP {test_response.status_code})")
    except requests.exceptions.ConnectionError as conn_exc:
        raise RuntimeError(f"Cannot connect to Ollama at {base_url}. Is it running? Error: {conn_exc}")
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Ollama connection timeout at {base_url}. Service may be slow or unreachable.")
    except Exception as conn_exc:
        raise RuntimeError(f"Ollama connectivity check failed at {base_url}: {conn_exc}")
    
    prompt = _build_ollama_json_plan_prompt(
        question=question,
        schema=schema,
        table=table,
        available_columns=available_columns,
    )
    heuristic_sql = _nl_to_sql_template(
        question=question,
        schema=schema,
        table=table,
        column_mapping=_build_column_mapping(available_columns),
    )

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "format": {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "chart_type": {"type": "string"},
                "x_axis": {"type": "string"},
                "y_axis": {"type": "string"},
                "color": {"type": "string"},
                "title": {"type": "string"},
            },
            "required": ["sql", "chart_type", "x_axis", "y_axis", "color", "title"],
        },
    }
    used_model = model

    def _post(req_payload: dict) -> requests.Response:
        return requests.post(
            f"{base_url}/api/chat",
            json=req_payload,
            headers=headers,
            timeout=timeout_sec,
        )

    try:
        response = _post(payload)
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Ollama request timeout ({timeout_sec}s). Try increasing OLLAMA_TIMEOUT_SEC.")
    except requests.exceptions.ConnectionError as conn_exc:
        raise RuntimeError(f"Cannot reach Ollama at {base_url} during generation: {conn_exc}")
    except Exception as post_exc:
        raise RuntimeError(f"Ollama request failed: {post_exc}")
    
    if response.status_code == 404 and ":" in model:
        # Try fallback model name
        fallback_model = model.split(":", 1)[0].strip()
        if fallback_model:
            payload["model"] = fallback_model
            try:
                retry = _post(payload)
                if retry.status_code < 400:
                    response = retry
                    used_model = fallback_model
            except Exception:
                pass
    
    if response.status_code >= 400 and "format" in payload:
        # Try without JSON format requirement
        retry_payload = dict(payload)
        retry_payload.pop("format", None)
        try:
            retry = _post(retry_payload)
            if retry.status_code < 400:
                response = retry
        except Exception:
            pass
    
    if response.status_code >= 400:
        error_text = response.text[:200] if response.text else "No error details"
        raise RuntimeError(f"Ollama API error (HTTP {response.status_code}): {error_text}. Model: {model}")

    try:
        data = response.json()
    except Exception as json_exc:
        raise RuntimeError(f"Cannot parse Ollama response as JSON: {json_exc}")
    
    raw = str(data.get("message", {}).get("content", ""))
    if not raw:
        raise RuntimeError("Ollama API returned empty response. Check model name and OLLAMA availability.")

    try:
        parsed = _extract_json_object(raw)
    except Exception as parse_exc:
        raise RuntimeError(f"Cannot parse structured output from Ollama: {parse_exc}")
    
    sql_candidates = [
        parsed.get("sql"),
        parsed.get("query"),
        parsed.get("statement"),
        parsed.get("select_sql"),
    ]
    sql_text = next((str(v) for v in sql_candidates if isinstance(v, str) and v.strip()), "")
    sql = _sanitize_extracted_sql(_extract_first_sql_query(sql_text or raw))
    if (not sql) or (re.search(r"\b(select|with)\b", sql, flags=re.IGNORECASE) is None):
        sql_prompt = _build_ollama_sql_only_prompt(
            question=question,
            schema=schema,
            table=table,
            available_columns=available_columns,
        )
        sql_payload = {
            "model": used_model,
            "messages": [{"role": "user", "content": sql_prompt}],
            "stream": False,
        }
        try:
            sql_resp = _post(sql_payload)
            if sql_resp.status_code < 400:
                sql_raw = str(sql_resp.json().get("message", {}).get("content", ""))
                sql = _sanitize_extracted_sql(_extract_first_sql_query(sql_raw))
        except Exception:
            pass

    if (not sql) or (re.search(r"\b(select|with)\b", sql, flags=re.IGNORECASE) is None):
        sql = _sanitize_extracted_sql(_extract_first_sql_query(heuristic_sql))

    plan = {
        "sql": sql,
        "chart_type": _normalize_ai_chart_type(str(parsed.get("chart_type", "auto"))),
        "x_axis": str(parsed.get("x_axis", "") or "").strip(),
        "y_axis": str(parsed.get("y_axis", "") or "").strip(),
        "color": str(parsed.get("color", "") or "").strip(),
        "title": str(parsed.get("title", "") or "").strip(),
    }
    return plan, used_model


def _nl_to_sql_template(
    question: str,
    schema: str,
    table: str,
    column_mapping: dict[str, str],
) -> str:
    fq_table = _target_fq_table(schema, table)
    q = question.lower().strip()

    plant_col = column_mapping.get("plant_name") or column_mapping.get("plant_id")
    date_col = column_mapping.get("date")
    oee_col = column_mapping.get("oee_normalized")
    avail_col = column_mapping.get("availability")
    perf_col = column_mapping.get("performance")
    qual_col = column_mapping.get("quality")
    downtime_col = column_mapping.get("downtime_min")
    runtime_col = column_mapping.get("run_time_min")
    units_col = column_mapping.get("total_units")
    good_units_col = column_mapping.get("good_units")
    source_col = "data_source_type" if "data_source_type" in (column_mapping or {}) else None

    # OEE Gap Analysis (reported vs normalized)
    if "oee gap" in q or "oee inconsistency" in q or "gap" in q:
        if oee_col and avail_col and perf_col and qual_col:
            oee = _quote_ident(oee_col)
            av = _quote_ident(avail_col)
            pf = _quote_ident(perf_col)
            ql = _quote_ident(qual_col)
            plant_name = _quote_ident(plant_col) if plant_col else "\"plant_id\""
            date = _quote_ident(date_col) if date_col else "\"date\""
            return f"""
SELECT {date} AS "date",
       {plant_name} AS "plant",
       ROUND({oee}, 4) AS "oee_normalized",
       ROUND({av} * {pf} * {ql}, 4) AS "oee_calculated",
       ROUND(ABS({oee} - ({av} * {pf} * {ql})), 4) AS "oee_gap",
       {av}, {pf}, {ql}
FROM {fq_table}
WHERE {oee} IS NOT NULL
ORDER BY {date} DESC, ABS({oee} - ({av} * {pf} * {ql})) DESC
LIMIT 500
""".strip()

    # Availability/Performance/Quality Breakdown
    if ("availability" in q or "performance" in q or "quality" in q) and ("breakdown" in q or "component" in q):
        if avail_col and perf_col and qual_col and plant_col:
            plant_name = _quote_ident(plant_col)
            av = _quote_ident(avail_col)
            pf = _quote_ident(perf_col)
            ql = _quote_ident(qual_col)
            return f"""
SELECT {plant_name} AS "plant",
       ROUND(AVG({av}), 4) AS "avg_availability",
       ROUND(AVG({pf}), 4) AS "avg_performance",
       ROUND(AVG({ql}), 4) AS "avg_quality",
       ROUND(AVG({av}) * AVG({pf}) * AVG({ql}), 4) AS "combined_oee",
       COUNT(*) AS "record_count"
FROM {fq_table}
GROUP BY {plant_name}
ORDER BY ROUND(AVG({av}) * AVG({pf}) * AVG({ql}), 4) DESC
LIMIT 200
""".strip()

    # Data Completeness by Source Type
    if "data quality" in q or "completeness" in q or "missing" in q:
        if source_col:
            return f"""
SELECT "data_source_type" AS "source_type",
       COUNT(*) AS "total_rows",
       ROUND(100.0 * SUM(CASE WHEN "availability" IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS "availability_completeness",
       ROUND(100.0 * SUM(CASE WHEN "performance" IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS "performance_completeness",
       ROUND(100.0 * SUM(CASE WHEN "quality" IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS "quality_completeness",
       ROUND(100.0 * SUM(CASE WHEN "oee_normalized" IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS "oee_completeness"
FROM {fq_table}
GROUP BY "data_source_type"
ORDER BY "total_rows" DESC
""".strip()

    # Plant Performance Trend
    if plant_col and oee_col and date_col and ("trend" in q or "over time" in q or "daily" in q or "time series" in q):
        plant = _quote_ident(plant_col)
        oee = _quote_ident(oee_col)
        date = _quote_ident(date_col)
        return f"""
SELECT {date} AS "date",
       {plant} AS "plant",
       ROUND(AVG({oee}), 4) AS "avg_oee"
FROM {fq_table}
WHERE {oee} IS NOT NULL AND {date} IS NOT NULL
GROUP BY {date}, {plant}
ORDER BY {date}, {plant}
LIMIT 1000
""".strip()

    # Top/Bottom Plants by OEE
    if ("top" in q or "best" in q or "worst" in q or "bottom" in q) and plant_col and oee_col:
        plant = _quote_ident(plant_col)
        oee = _quote_ident(oee_col)
        direction = "DESC" if ("top" in q or "best" in q) else "ASC"
        return f"""
SELECT {plant} AS "plant",
       ROUND(AVG({oee}), 4) AS "avg_oee",
       ROUND(MIN({oee}), 4) AS "min_oee",
       ROUND(MAX({oee}), 4) AS "max_oee",
       COUNT(*) AS "record_count"
FROM {fq_table}
WHERE {oee} IS NOT NULL
GROUP BY {plant}
ORDER BY AVG({oee}) {direction}
LIMIT 15
""".strip()

    # Downtime Analysis
    if ("downtime" in q or "stop" in q or "maintenance" in q) and downtime_col and plant_col:
        plant = _quote_ident(plant_col)
        downtime = _quote_ident(downtime_col)
        return f"""
SELECT {plant} AS "plant",
       ROUND(AVG({downtime}), 2) AS "avg_downtime_min",
       ROUND(SUM({downtime}), 2) AS "total_downtime_min",
       COUNT(*) AS "record_count"
FROM {fq_table}
WHERE {downtime} IS NOT NULL AND {downtime} > 0
GROUP BY {plant}
ORDER BY SUM({downtime}) DESC
LIMIT 100
""".strip()

    # Yield/Quality Analysis
    if ("yield" in q or "scrap" in q or "defect" in q or "quality" in q) and units_col and good_units_col:
        units = _quote_ident(units_col)
        good = _quote_ident(good_units_col)
        plant_name = _quote_ident(plant_col) if plant_col else "\"plant_id\""
        return f"""
SELECT {plant_name} AS "plant",
       SUM({good}) AS "total_good_units",
       SUM({units}) - SUM({good}) AS "total_defective_units",
       SUM({units}) AS "total_units",
       ROUND(100.0 * SUM({good}) / SUM({units}), 2) AS "yield_rate_pct"
FROM {fq_table}
WHERE {units} > 0
GROUP BY {plant_name}
ORDER BY ROUND(100.0 * SUM({good}) / SUM({units}), 2) DESC
LIMIT 100
""".strip()

    # Source Type Comparison
    if source_col and plant_col and ("source" in q or "mes type" in q or "data source" in q):
        source = "data_source_type"
        plant = _quote_ident(plant_col)
        oee = _quote_ident(oee_col) if oee_col else "\"oee_normalized\""
        return f"""
SELECT "{source}" AS "source_type",
       {plant} AS "plant",
       COUNT(*) AS "record_count",
       ROUND(AVG({oee}), 4) AS "avg_oee"
FROM {fq_table}
WHERE {oee} IS NOT NULL
GROUP BY "{source}", {plant}
ORDER BY "{source}", AVG({oee}) DESC
LIMIT 300
""".strip()

    # Default: OEE by Plant
    if plant_col and oee_col:
        plant = _quote_ident(plant_col)
        oee = _quote_ident(oee_col)
        return f"""
SELECT {plant} AS "plant",
       ROUND(AVG({oee}), 4) AS "avg_oee",
       COUNT(*) AS "record_count"
FROM {fq_table}
WHERE {oee} IS NOT NULL
GROUP BY {plant}
ORDER BY 2 DESC
LIMIT 50
""".strip()

    return f"SELECT * FROM {fq_table} LIMIT 200"


def _pick_metric_column(df: pd.DataFrame) -> str | None:
    preferred = [
        "avg_oee_normalized",
        "oee_normalized",
        "oee_reported",
        "availability",
        "performance",
        "quality",
        "downtime_min",
        "run_time_min",
        "planned_time_min",
        "total_units",
        "good_units",
        "defective_units",
    ]
    lower_to_actual = {c.lower(): c for c in df.columns}
    for name in preferred:
        if name in lower_to_actual and pd.api.types.is_numeric_dtype(df[lower_to_actual[name]]):
            return lower_to_actual[name]

    for col in df.select_dtypes(include=["number"]).columns:
        if "id" not in col.lower():
            return col
    return None


def _pick_date_column(df: pd.DataFrame) -> str | None:
    lower_to_actual = {c.lower(): c for c in df.columns}
    for name in ["date", "day", "timestamp", "event_time"]:
        if name in lower_to_actual:
            col = lower_to_actual[name]
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().any():
                return col

    for col in df.columns:
        parsed = pd.to_datetime(df[col], errors="coerce")
        if parsed.notna().sum() >= max(3, len(df) // 5):
            return col
    return None


def _pick_category_column(df: pd.DataFrame) -> str | None:
    preferred = ["plant", "plant_name", "plant_id", "country", "shift", "data_source_type"]
    lower_to_actual = {c.lower(): c for c in df.columns}
    for name in preferred:
        if name in lower_to_actual:
            return lower_to_actual[name]

    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or str(df[col].dtype).startswith("string"):
            return col
    return None


def _numeric_insight_lines(df: pd.DataFrame, max_metrics: int = 4) -> list[str]:
    lines: list[str] = []
    numeric_cols = [
        c for c in df.select_dtypes(include=["number"]).columns
        if "id" not in c.lower()
    ]
    if not numeric_cols:
        return lines

    preferred_order = [
        "oee_normalized",
        "oee_reported",
        "availability",
        "performance",
        "quality",
        "downtime_min",
        "run_time_min",
        "planned_time_min",
        "total_units",
        "good_units",
        "defective_units",
        "rejected_units",
    ]
    ordered = [c for c in preferred_order if c in numeric_cols]
    ordered.extend([c for c in numeric_cols if c not in ordered])

    for col in ordered[:max_metrics]:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        if s.empty:
            continue
        lines.append(
            f"`{col}` mean {s.mean():.4f}, median {s.median():.4f}, p90 {s.quantile(0.9):.4f}."
        )
    return lines


def _build_insights(
    df: pd.DataFrame,
    question: str,
    metric_override: str | None = None,
) -> list[str]:
    insights: list[str] = [f"📝 Question: `{question}`"]
    insights.append(f"📊 Returned {len(df):,} rows.")

    metric_col = metric_override if metric_override in df.columns else _pick_metric_column(df)
    date_col = _pick_date_column(df)
    cat_col = _pick_category_column(df)

    # OEE-specific insights
    if "oee_normalized" in df.columns and "oee_normalized" in question.lower():
        oee_series = pd.to_numeric(df["oee_normalized"], errors="coerce").dropna()
        if not oee_series.empty:
            mean_oee = oee_series.mean()
            insights.append(f"📈 Average OEE = {mean_oee:.1%} (min={oee_series.min():.1%}, max={oee_series.max():.1%})")
            if mean_oee < 0.65:
                insights.append("🚨 Warning: Average OEE is below 65% - investigate bottlenecks")
            elif mean_oee < 0.80:
                insights.append("⚠️ Note: Room for improvement - target is typically 80%+")
            else:
                insights.append("✅ Strong performance - OEE exceeds industry benchmark")

    # OEE Gap insights
    if {"availability", "performance", "quality", "oee_normalized"}.issubset(df.columns):
        work = df.copy()
        work["availability"] = pd.to_numeric(work["availability"], errors="coerce")
        work["performance"] = pd.to_numeric(work["performance"], errors="coerce")
        work["quality"] = pd.to_numeric(work["quality"], errors="coerce")
        work["oee_normalized"] = pd.to_numeric(work["oee_normalized"], errors="coerce")
        work["oee_calc"] = work["availability"] * work["performance"] * work["quality"]
        gap = (work["oee_normalized"] - work["oee_calc"]).abs()
        if gap.notna().any():
            avg_gap = gap.mean()
            if avg_gap > 0.05:
                insights.append(f"⚠️ OEE Inconsistency: Gap between reported and calculated is {avg_gap:.1%}")

    if metric_col:
        series = pd.to_numeric(df[metric_col], errors="coerce").dropna()
        if not series.empty:
            insights.append(
                f"📊 Average `{metric_col}` = {series.mean():.2f} (min={series.min():.2f}, max={series.max():.2f}, σ={series.std():.2f})"
            )

    if metric_col and cat_col and cat_col in df.columns:
        group_df = (
            df.dropna(subset=[cat_col])
            .assign(__metric=pd.to_numeric(df[metric_col], errors="coerce"))
            .dropna(subset=["__metric"])
            .groupby(cat_col, as_index=False)["__metric"]
            .mean()
            .sort_values("__metric", ascending=False)
        )
        if not group_df.empty:
            top = group_df.iloc[0]
            bottom = group_df.iloc[-1]
            spread = top["__metric"] - bottom["__metric"]
            insights.append(
                f"🏆 Top `{cat_col}`: `{top[cat_col]}` ({top['__metric']:.2f})"
            )
            if spread > series.std() if not series.empty else False:
                insights.append(f"📉 Significant variation: {spread:.2f} between best and worst")

    if metric_col and date_col:
        work = df.copy()
        work["__date"] = pd.to_datetime(work[date_col], errors="coerce")
        work["__metric"] = pd.to_numeric(work[metric_col], errors="coerce")
        trend = (
            work.dropna(subset=["__date", "__metric"])
            .groupby("__date", as_index=False)["__metric"]
            .mean()
            .sort_values("__date")
        )
        if len(trend) >= 2:
            first = float(trend["__metric"].iloc[0])
            last = float(trend["__metric"].iloc[-1])
            delta = last - first
            direction = "📈 increased" if delta >= 0 else "📉 decreased"
            insights.append(
                f"📊 Trend: `{metric_col}` {direction} by {abs(delta):.2f} from first to last date"
            )

    insights.extend(_numeric_insight_lines(df, max_metrics=3))

    return insights


def _render_auto_chart(df: pd.DataFrame) -> None:
    metric_col = _pick_metric_column(df)
    date_col = _pick_date_column(df)
    cat_col = _pick_category_column(df)

    if metric_col and date_col:
        work = df.copy()
        work["__date"] = pd.to_datetime(work[date_col], errors="coerce")
        work["__metric"] = pd.to_numeric(work[metric_col], errors="coerce")
        work = work.dropna(subset=["__date", "__metric"])
        if work.empty:
            st.info("No chartable rows for date-based trend.")
            return

        if cat_col and cat_col in work.columns and work[cat_col].nunique(dropna=True) <= 12:
            chart_df = (
                work.groupby(["__date", cat_col], as_index=False)["__metric"]
                .mean()
                .pivot(index="__date", columns=cat_col, values="__metric")
                .sort_index()
            )
            st.line_chart(chart_df)
            return

        chart_df = (
            work.groupby("__date", as_index=False)["__metric"].mean().sort_values("__date")
        )
        st.line_chart(chart_df.set_index("__date")[["__metric"]])
        return

    if metric_col and cat_col:
        work = df.copy()
        work["__metric"] = pd.to_numeric(work[metric_col], errors="coerce")
        bar_df = (
            work.dropna(subset=[cat_col, "__metric"])
            .groupby(cat_col, as_index=False)["__metric"]
            .mean()
            .sort_values("__metric", ascending=False)
            .head(20)
            .set_index(cat_col)
        )
        if bar_df.empty:
            st.info("No chartable rows for category comparison.")
            return
        st.bar_chart(bar_df)
        return

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if len(numeric_cols) >= 2:
        st.line_chart(df[numeric_cols[:2]])
        return

    st.info("No suitable columns available for automatic charting.")


def _explicit_chart_type_from_question(question: str) -> str | None:
    q = question.lower()
    if any(word in q for word in ["line chart", "line graph", "lineplot", "line plot"]):
        return "line"
    if any(word in q for word in ["bar chart", "bar graph", "column chart", "column graph"]):
        return "bar"
    if any(word in q for word in ["circular", "pie chart", "pie graph", "donut", "doughnut"]):
        return "pie"
    if any(word in q for word in ["scatter", "scatter plot", "bubble"]):
        return "scatter"
    if any(word in q for word in ["histogram", "distribution", "spread"]):
        return "hist"
    if any(word in q for word in ["heatmap", "heat map"]):
        return "heatmap"
    return None


def _chart_intent_from_question(question: str) -> str:
    explicit = _explicit_chart_type_from_question(question)
    if explicit:
        return explicit

    q = question.lower()
    
    # OEE Gap Analysis → Comparison view
    if "gap" in q or "inconsistency" in q or "diff" in q:
        return "bar"
    
    # Trend Analysis → Line chart
    if any(word in q for word in ["trend", "over time", "daily", "timeline", "month", "week", "growth", "change"]):
        return "line"
    
    # Rankings/Comparisons → Bar chart
    if any(word in q for word in ["compare", "top", "best", "worst", "rank", "by plant", "plant-wise", "which", "leader", "laggard"]):
        return "bar"
    
    # Composition/Breakdown → Pie/Stacked bar
    if any(word in q for word in ["share", "proportion", "composition", "split", "percentage", "breakdown", "component", "a×p×q", "apq"]):
        return "stacked_bar"
    
    # Correlation/Relationship → Scatter
    if any(word in q for word in ["correlation", "relationship", "vs", "against", "impact", "depends", "scatter"]):
        return "scatter"
    
    # Distribution → Histogram
    if any(word in q for word in ["pattern", "distribution", "spread", "variance", "range", "histogram"]):
        return "hist"
    
    # Data Quality → Heatmap
    if any(word in q for word in ["quality", "completeness", "missing", "coverage", "matrix"]):
        return "heatmap"
    
    return "auto"


def _build_chart_candidates(
    df: pd.DataFrame,
    metric_override: str | None = None,
    top_n: int = 20,
) -> dict[str, tuple[str, alt.Chart]]:
    candidates: dict[str, tuple[str, alt.Chart]] = {}
    metric_col = metric_override if metric_override in df.columns else _pick_metric_column(df)
    date_col = _pick_date_column(df)
    cat_col = _pick_category_column(df)

    if metric_col and date_col:
        work = df.copy()
        work["__date"] = pd.to_datetime(work[date_col], errors="coerce")
        work["__metric"] = pd.to_numeric(work[metric_col], errors="coerce")
        work = work.dropna(subset=["__date", "__metric"])
        if not work.empty:
            if cat_col and cat_col in work.columns and work[cat_col].nunique(dropna=True) <= 10:
                chart_df = (
                    work.groupby(["__date", cat_col], as_index=False)["__metric"]
                    .mean()
                    .sort_values("__date")
                )
                chart = (
                    alt.Chart(chart_df)
                    .mark_line(point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("__date:T", title="Date"),
                        y=alt.Y("__metric:Q", title=metric_col),
                        color=alt.Color(f"{cat_col}:N", title=cat_col),
                        tooltip=[
                            alt.Tooltip("__date:T", title="Date"),
                            alt.Tooltip(f"{cat_col}:N", title=cat_col),
                            alt.Tooltip("__metric:Q", title=metric_col, format=".4f"),
                        ],
                    )
                    .properties(height=280)
                    .interactive()
                )
            else:
                chart_df = work.groupby("__date", as_index=False)["__metric"].mean().sort_values("__date")
                chart = (
                    alt.Chart(chart_df)
                    .mark_line(point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("__date:T", title="Date"),
                        y=alt.Y("__metric:Q", title=metric_col),
                        tooltip=[
                            alt.Tooltip("__date:T", title="Date"),
                            alt.Tooltip("__metric:Q", title=metric_col, format=".4f"),
                        ],
                    )
                    .properties(height=280)
                    .interactive()
                )
            candidates["line"] = ("Line", chart)

    if metric_col and cat_col:
        work = df.copy()
        work["__metric"] = pd.to_numeric(work[metric_col], errors="coerce")
        chart_df = (
            work.dropna(subset=[cat_col, "__metric"])
            .groupby(cat_col, as_index=False)["__metric"]
            .mean()
            .sort_values("__metric", ascending=False)
            .head(max(3, min(50, int(top_n))))
        )
        if not chart_df.empty:
            bar = (
                alt.Chart(chart_df)
                .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
                .encode(
                    x=alt.X(f"{cat_col}:N", sort=chart_df[cat_col].tolist(), title=cat_col),
                    y=alt.Y("__metric:Q", title=f"Avg {metric_col}"),
                    color=alt.Color("__metric:Q", scale=alt.Scale(scheme="teals"), title=f"Avg {metric_col}"),
                    tooltip=[
                        alt.Tooltip(f"{cat_col}:N", title=cat_col),
                        alt.Tooltip("__metric:Q", title=f"Avg {metric_col}", format=".4f"),
                    ],
                )
                .properties(height=280)
            )
            candidates["bar"] = ("Comparison", bar)

            # Explicit line-chart fallback when user asks line but result has no date axis.
            line_over_category = (
                alt.Chart(chart_df)
                .mark_line(point=True, strokeWidth=2)
                .encode(
                    x=alt.X(f"{cat_col}:N", sort=chart_df[cat_col].tolist(), title=cat_col),
                    y=alt.Y("__metric:Q", title=f"Avg {metric_col}"),
                    tooltip=[
                        alt.Tooltip(f"{cat_col}:N", title=cat_col),
                        alt.Tooltip("__metric:Q", title=f"Avg {metric_col}", format=".4f"),
                    ],
                )
                .properties(height=280)
            )
            candidates.setdefault("line", ("Line", line_over_category))

            pie_df = chart_df.head(min(10, len(chart_df))).copy()
            pie_df["label"] = pie_df[cat_col].astype(str)
            if len(chart_df) > len(pie_df):
                others_val = float(chart_df["__metric"].iloc[len(pie_df):].sum())
                if others_val > 0:
                    pie_df = pd.concat(
                        [
                            pie_df,
                            pd.DataFrame([{cat_col: "Others", "__metric": others_val, "label": "Others"}]),
                        ],
                        ignore_index=True,
                    )
            pie = (
                alt.Chart(pie_df)
                .mark_arc(innerRadius=60)
                .encode(
                    theta=alt.Theta("__metric:Q", title=f"Avg {metric_col}"),
                    color=alt.Color("label:N", title=cat_col),
                    tooltip=[
                        alt.Tooltip("label:N", title=cat_col),
                        alt.Tooltip("__metric:Q", title=f"Avg {metric_col}", format=".4f"),
                    ],
                )
                .properties(height=280)
            )
            candidates["pie"] = ("Circular", pie)

    if metric_col:
        hist_df = df.copy()
        hist_df["__metric"] = pd.to_numeric(hist_df[metric_col], errors="coerce")
        hist_df = hist_df.dropna(subset=["__metric"])
        if not hist_df.empty:
            hist = (
                alt.Chart(hist_df)
                .mark_bar()
                .encode(
                    x=alt.X("__metric:Q", bin=alt.Bin(maxbins=25), title=metric_col),
                    y=alt.Y("count():Q", title="Count"),
                    tooltip=[alt.Tooltip("count():Q", title="Rows")],
                )
                .properties(height=280)
            )
            candidates["hist"] = ("Distribution", hist)

    numeric_cols = [c for c in df.select_dtypes(include=["number"]).columns if "id" not in c.lower()]
    if len(numeric_cols) >= 2:
        preferred = ["performance", "quality", "availability", "oee_normalized"]
        picked = [c for c in preferred if c in numeric_cols]
        if len(picked) >= 2:
            x_col, y_col = picked[0], picked[1]
        else:
            x_col, y_col = numeric_cols[:2]
        sc_df = df.copy().dropna(subset=[x_col, y_col])
        if not sc_df.empty:
            scatter = (
                alt.Chart(sc_df)
                .mark_circle(opacity=0.7, size=80)
                .encode(
                    x=alt.X(f"{x_col}:Q", title=x_col),
                    y=alt.Y(f"{y_col}:Q", title=y_col),
                    color=alt.Color(f"{cat_col}:N", title=cat_col) if cat_col else alt.value("#0f766e"),
                    tooltip=[
                        alt.Tooltip(f"{x_col}:Q", format=".4f"),
                        alt.Tooltip(f"{y_col}:Q", format=".4f"),
                    ],
                )
                .properties(height=280)
                .interactive()
            )
            candidates["scatter"] = ("Relationship", scatter)

    if metric_col and date_col and cat_col and cat_col in df.columns:
        heat_df = df.copy()
        heat_df["__date"] = pd.to_datetime(heat_df[date_col], errors="coerce")
        heat_df["__metric"] = pd.to_numeric(heat_df[metric_col], errors="coerce")
        heat_df = (
            heat_df.dropna(subset=["__date", "__metric", cat_col])
            .groupby(["__date", cat_col], as_index=False)["__metric"]
            .mean()
        )
        if not heat_df.empty and heat_df[cat_col].nunique(dropna=True) <= 20:
            heat = (
                alt.Chart(heat_df)
                .mark_rect()
                .encode(
                    x=alt.X("__date:T", title="Date"),
                    y=alt.Y(f"{cat_col}:N", title=cat_col),
                    color=alt.Color("__metric:Q", title=metric_col, scale=alt.Scale(scheme="yellowgreenblue")),
                    tooltip=[
                        alt.Tooltip("__date:T", title="Date"),
                        alt.Tooltip(f"{cat_col}:N", title=cat_col),
                        alt.Tooltip("__metric:Q", title=metric_col, format=".4f"),
                    ],
                )
                .properties(height=280)
            )
            candidates["heatmap"] = ("Heatmap", heat)

    return candidates


def _render_dynamic_chart_tiles(
    df: pd.DataFrame,
    question: str,
    compact: bool,
    key_prefix: str,
    metric_override: str | None = None,
    top_n: int = 20,
) -> None:
    candidates = _build_chart_candidates(df, metric_override=metric_override, top_n=top_n)
    if not candidates:
        _render_auto_chart(df)
        return

    explicit_intent = _explicit_chart_type_from_question(question)
    intent = _chart_intent_from_question(question)
    keys = list(candidates.keys())
    default_key = intent if intent in candidates else keys[0]
    selector_key = f"{key_prefix}_primary_chart"

    if explicit_intent and explicit_intent in candidates:
        st.session_state[selector_key] = explicit_intent
    elif selector_key not in st.session_state:
        st.session_state[selector_key] = default_key

    selected_key = st.selectbox(
        "Primary Chart",
        options=keys,
        format_func=lambda k: candidates[k][0],
        index=keys.index(default_key),
        key=selector_key,
    )
    selected_label, selected_chart = candidates[selected_key]
    st.caption(f"Primary: {selected_label}")
    st.altair_chart(selected_chart, width='stretch')

    if compact:
        return

    remaining = [(k, v) for k, v in candidates.items() if k != selected_key]
    if not remaining:
        return

    st.markdown("**Additional Views**")
    for i in range(0, len(remaining), 2):
        row = remaining[i:i + 2]
        cols = st.columns(2, gap="large")
        for col, (_, (label, chart)) in zip(cols, row):
            with col:
                with st.container(border=True):
                    st.caption(label)
                    st.altair_chart(chart, width='stretch')


def _resolve_column_name(requested: str | None, columns: list[str]) -> str | None:
    if not requested:
        return None
    token = requested.strip().strip('"').strip()
    if not token:
        return None

    lower_to_actual = {c.lower(): c for c in columns}
    if token.lower() in lower_to_actual:
        return lower_to_actual[token.lower()]

    close = difflib.get_close_matches(token.lower(), list(lower_to_actual.keys()), n=1, cutoff=0.75)
    if close:
        return lower_to_actual[close[0]]
    return None


def _render_ai_plan_chart(
    df: pd.DataFrame,
    question: str,
    key_prefix: str,
    ai_plan: dict[str, str],
    metric_override: str | None = None,
) -> None:
    if df.empty:
        st.info("No rows to chart.")
        return

    chart_type = _normalize_ai_chart_type(ai_plan.get("chart_type"))
    title = ai_plan.get("title", "").strip() or f"AI Chart ({chart_type})"

    x_col = _resolve_column_name(ai_plan.get("x_axis"), list(df.columns))
    y_col = _resolve_column_name(ai_plan.get("y_axis"), list(df.columns))
    color_col = _resolve_column_name(ai_plan.get("color"), list(df.columns))

    fallback_metric = metric_override if metric_override in df.columns else _pick_metric_column(df)
    fallback_date = _pick_date_column(df)
    fallback_cat = _pick_category_column(df)
    num_cols = [c for c in df.select_dtypes(include=["number"]).columns if "id" not in c.lower()]

    if not y_col and fallback_metric:
        y_col = fallback_metric
    if not x_col and chart_type in {"line", "heatmap"} and fallback_date:
        x_col = fallback_date
    if not x_col and chart_type in {"bar", "pie"} and fallback_cat:
        x_col = fallback_cat
    if not color_col and fallback_cat and fallback_cat != x_col:
        color_col = fallback_cat

    try:
        if chart_type == "line":
            if not x_col or not y_col:
                raise ValueError("Missing x or y for line chart.")
            work = df.copy()
            work["__x"] = pd.to_datetime(work[x_col], errors="coerce")
            work["__y"] = pd.to_numeric(work[y_col], errors="coerce")
            work = work.dropna(subset=["__x", "__y"])
            if work.empty:
                raise ValueError("No valid rows for line chart.")
            if color_col and color_col in work.columns and work[color_col].nunique(dropna=True) <= 12:
                chart_df = work.groupby(["__x", color_col], as_index=False)["__y"].mean()
                chart = (
                    alt.Chart(chart_df)
                    .mark_line(point=True, strokeWidth=2.4)
                    .encode(
                        x=alt.X("__x:T", title=x_col),
                        y=alt.Y("__y:Q", title=y_col),
                        color=alt.Color(f"{color_col}:N", title=color_col),
                        tooltip=[
                            alt.Tooltip("__x:T", title=x_col),
                            alt.Tooltip(f"{color_col}:N", title=color_col),
                            alt.Tooltip("__y:Q", title=y_col, format=".4f"),
                        ],
                    )
                    .properties(height=320, title=title)
                    .interactive()
                )
            else:
                chart_df = work.groupby("__x", as_index=False)["__y"].mean()
                chart = (
                    alt.Chart(chart_df)
                    .mark_line(point=True, strokeWidth=2.4, color="#0f766e")
                    .encode(
                        x=alt.X("__x:T", title=x_col),
                        y=alt.Y("__y:Q", title=y_col),
                        tooltip=[
                            alt.Tooltip("__x:T", title=x_col),
                            alt.Tooltip("__y:Q", title=y_col, format=".4f"),
                        ],
                    )
                    .properties(height=320, title=title)
                    .interactive()
                )
            st.altair_chart(chart, width='stretch')
            return

        if chart_type in {"bar", "bar_sorted"}:
            if not x_col or not y_col:
                raise ValueError("Missing x or y for bar chart.")
            work = df.copy()
            work["__y"] = pd.to_numeric(work[y_col], errors="coerce")
            bar_df = (
                work.dropna(subset=[x_col, "__y"])
                .groupby(x_col, as_index=False)["__y"]
                .mean()
            )
            if chart_type == "bar_sorted":
                bar_df = bar_df.sort_values("__y", ascending=False)
            else:
                bar_df = bar_df.sort_values(x_col, ascending=True)
            bar_df = bar_df.head(25)
            if bar_df.empty:
                raise ValueError("No valid rows for bar chart.")
            chart = (
                alt.Chart(bar_df)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X(f"{x_col}:N", sort=bar_df[x_col].tolist(), title=x_col),
                    y=alt.Y("__y:Q", title=y_col),
                    color=alt.Color("__y:Q", scale=alt.Scale(scheme="teals"), title=y_col),
                    tooltip=[
                        alt.Tooltip(f"{x_col}:N", title=x_col),
                        alt.Tooltip("__y:Q", title=y_col, format=".4f"),
                    ],
                )
                .properties(height=320, title=title)
            )
            st.altair_chart(chart, width='stretch')
            return

        if chart_type == "stacked_bar":
            if not x_col:
                x_col = _plant_column(df) or _pick_category_column(df)
            if not y_col:
                y_col = fallback_metric
            if not color_col:
                if "data_source_type" in df.columns and x_col != "data_source_type":
                    color_col = "data_source_type"
                elif "shift" in df.columns and x_col != "shift":
                    color_col = "shift"
                else:
                    color_col = _pick_category_column(df)

            if not x_col or not y_col or not color_col:
                raise ValueError("Missing fields for stacked bar chart.")

            work = df.copy()
            work["__y"] = pd.to_numeric(work[y_col], errors="coerce")
            stack_df = (
                work.dropna(subset=[x_col, color_col, "__y"])
                .groupby([x_col, color_col], as_index=False)["__y"]
                .mean()
            )
            if stack_df.empty:
                raise ValueError("No valid rows for stacked bar chart.")
            chart = (
                alt.Chart(stack_df)
                .mark_bar()
                .encode(
                    x=alt.X(f"{x_col}:N", title=x_col),
                    y=alt.Y("__y:Q", title=y_col, stack="zero"),
                    color=alt.Color(f"{color_col}:N", title=color_col),
                    tooltip=[
                        alt.Tooltip(f"{x_col}:N", title=x_col),
                        alt.Tooltip(f"{color_col}:N", title=color_col),
                        alt.Tooltip("__y:Q", title=y_col, format=".4f"),
                    ],
                )
                .properties(height=320, title=title)
            )
            st.altair_chart(chart, width='stretch')
            return

        if chart_type == "pie":
            if not x_col or not y_col:
                raise ValueError("Missing x or y for pie chart.")
            work = df.copy()
            work["__y"] = pd.to_numeric(work[y_col], errors="coerce")
            pie_df = (
                work.dropna(subset=[x_col, "__y"])
                .groupby(x_col, as_index=False)["__y"]
                .sum()
                .sort_values("__y", ascending=False)
                .head(10)
            )
            if pie_df.empty:
                raise ValueError("No valid rows for pie chart.")
            chart = (
                alt.Chart(pie_df)
                .mark_arc(innerRadius=65)
                .encode(
                    theta=alt.Theta("__y:Q", title=y_col),
                    color=alt.Color(f"{x_col}:N", title=x_col),
                    tooltip=[
                        alt.Tooltip(f"{x_col}:N", title=x_col),
                        alt.Tooltip("__y:Q", title=y_col, format=".4f"),
                    ],
                )
                .properties(height=320, title=title)
            )
            st.altair_chart(chart, width='stretch')
            return

        if chart_type == "scatter":
            x_num = x_col if x_col in num_cols else None
            y_num = y_col if y_col in num_cols else None
            if not x_num and len(num_cols) >= 1:
                x_num = num_cols[0]
            if not y_num and len(num_cols) >= 2:
                y_num = num_cols[1]
            if not x_num or not y_num:
                raise ValueError("Missing numeric axes for scatter chart.")
            sc_df = df.dropna(subset=[x_num, y_num]).copy()
            if sc_df.empty:
                raise ValueError("No valid rows for scatter chart.")
            chart = (
                alt.Chart(sc_df)
                .mark_circle(opacity=0.75, size=85)
                .encode(
                    x=alt.X(f"{x_num}:Q", title=x_num),
                    y=alt.Y(f"{y_num}:Q", title=y_num),
                    color=alt.Color(f"{color_col}:N", title=color_col) if color_col else alt.value("#0f766e"),
                    tooltip=[
                        alt.Tooltip(f"{x_num}:Q", format=".4f"),
                        alt.Tooltip(f"{y_num}:Q", format=".4f"),
                    ],
                )
                .properties(height=320, title=title)
                .interactive()
            )
            st.altair_chart(chart, width='stretch')
            return

        if chart_type == "heatmap":
            if not x_col:
                x_col = fallback_date or fallback_cat
            if not y_col:
                y_col = fallback_cat
            metric_col = fallback_metric
            if not x_col or not y_col or not metric_col:
                raise ValueError("Missing fields for heatmap.")
            work = df.copy()
            work["__metric"] = pd.to_numeric(work[metric_col], errors="coerce")
            work = work.dropna(subset=[x_col, y_col, "__metric"])
            if work.empty:
                raise ValueError("No valid rows for heatmap.")
            if x_col == fallback_date:
                work["__x"] = pd.to_datetime(work[x_col], errors="coerce")
                work = work.dropna(subset=["__x"])
                heat_df = work.groupby(["__x", y_col], as_index=False)["__metric"].mean()
                x_enc = alt.X("__x:T", title=x_col)
                x_tip = alt.Tooltip("__x:T", title=x_col)
            else:
                heat_df = work.groupby([x_col, y_col], as_index=False)["__metric"].mean()
                x_enc = alt.X(f"{x_col}:N", title=x_col)
                x_tip = alt.Tooltip(f"{x_col}:N", title=x_col)
            chart = (
                alt.Chart(heat_df)
                .mark_rect()
                .encode(
                    x=x_enc,
                    y=alt.Y(f"{y_col}:N", title=y_col),
                    color=alt.Color("__metric:Q", title=metric_col, scale=alt.Scale(scheme="yellowgreenblue")),
                    tooltip=[
                        x_tip,
                        alt.Tooltip(f"{y_col}:N", title=y_col),
                        alt.Tooltip("__metric:Q", title=metric_col, format=".4f"),
                    ],
                )
                .properties(height=320, title=title)
            )
            st.altair_chart(chart, width='stretch')
            return

        if chart_type == "hist":
            metric_col = y_col if y_col in df.columns else fallback_metric
            if not metric_col:
                raise ValueError("Missing numeric field for histogram.")
            hist_df = df.copy()
            hist_df["__metric"] = pd.to_numeric(hist_df[metric_col], errors="coerce")
            hist_df = hist_df.dropna(subset=["__metric"])
            if hist_df.empty:
                raise ValueError("No valid rows for histogram.")
            chart = (
                alt.Chart(hist_df)
                .mark_bar()
                .encode(
                    x=alt.X("__metric:Q", bin=alt.Bin(maxbins=25), title=metric_col),
                    y=alt.Y("count():Q", title="Count"),
                    tooltip=[alt.Tooltip("count():Q", title="Rows")],
                )
                .properties(height=320, title=title)
            )
            st.altair_chart(chart, width='stretch')
            return
    except Exception:
        pass

    _render_question_driven_chart(
        df,
        question=question,
        key_prefix=f"{key_prefix}_fallback",
        metric_override=metric_override,
    )


def _render_question_driven_chart(
    df: pd.DataFrame,
    question: str,
    key_prefix: str,
    metric_override: str | None = None,
) -> None:
    intent = _chart_intent_from_question(question)
    candidates = _build_chart_candidates(df, metric_override=metric_override, top_n=20)
    if not candidates:
        _render_auto_chart(df)
        return

    chosen = intent if intent in candidates else ("line" if "line" in candidates else next(iter(candidates)))
    label, chart = candidates[chosen]
    st.caption(f"Chart type from question: {label}")
    st.altair_chart(chart, width='stretch')

    with st.expander("Switch Chart Type"):
        keys = list(candidates.keys())
        fallback = st.selectbox(
            "Chart",
            options=keys,
            format_func=lambda k: candidates[k][0],
            index=keys.index(chosen),
            key=f"{key_prefix}_chart_switch",
        )
        if fallback != chosen:
            alt_label, alt_chart = candidates[fallback]
            st.caption(f"Alternative: {alt_label}")
            st.altair_chart(alt_chart, width='stretch')


def _pick_metric_from_question(question: str, columns: list[str]) -> str | None:
    q = question.lower()
    metric_map = {
        "oee_normalized": ["oee", "normalized oee", "oee normalized"],
        "availability": ["availability", "uptime"],
        "performance": ["performance", "speed"],
        "quality": ["quality", "yield"],
        "downtime_min": ["downtime", "stoppage", "down time"],
        "run_time_min": ["run time", "runtime", "operating time"],
        "planned_time_min": ["planned time", "plan time"],
        "total_units": ["total units", "units produced", "production volume"],
        "good_units": ["good units", "good count"],
        "defective_units": ["defective", "reject", "rejected units", "scrap"],
    }
    for metric, aliases in metric_map.items():
        if metric in columns and any(alias in q for alias in aliases):
            return metric
    return None


def _extract_top_n_from_question(question: str) -> int | None:
    q = question.lower()
    match = re.search(r"\btop\s+(\d+)\b", q)
    if match:
        return max(3, min(50, int(match.group(1))))
    return None


def _apply_question_filters(
    df: pd.DataFrame,
    question: str,
) -> tuple[pd.DataFrame, list[str], str | None, int]:
    out = df.copy()
    q = question.lower()
    notes: list[str] = []

    metric_override = _pick_metric_from_question(q, list(out.columns))
    if metric_override:
        notes.append(f"Metric focused: `{metric_override}`")

    top_n = _extract_top_n_from_question(q) or 20
    if "top " in q:
        notes.append(f"Top-N requested: {top_n}")

    plant_col = _plant_column(out)
    if plant_col and plant_col in out.columns:
        values = [str(v) for v in out[plant_col].dropna().unique().tolist()]
        selected = [v for v in values if v.lower() in q]
        if not selected:
            m = re.search(r"\bplant\s*0*(\d+)\b", q)
            if m:
                plant_num = m.group(1)
                selected = [
                    v for v in values
                    if re.search(rf"\b0*{re.escape(plant_num)}\b", v, flags=re.IGNORECASE)
                ]
        if selected:
            out = out[out[plant_col].astype(str).isin(selected)]
            notes.append(f"Filtered `{plant_col}`: {', '.join(selected[:6])}")

    if "data_source_type" in out.columns:
        source_tokens = ["event", "kpi", "production", "time", "quality"]
        selected = [t for t in source_tokens if re.search(rf"\b{re.escape(t)}\b", q)]
        if selected:
            out = out[out["data_source_type"].astype(str).str.lower().isin(selected)]
            notes.append(f"Filtered `data_source_type`: {', '.join(selected)}")

    if "country" in out.columns:
        countries = [str(v) for v in out["country"].dropna().unique().tolist()]
        selected = [c for c in countries if c.lower() in q]
        if selected:
            out = out[out["country"].astype(str).isin(selected)]
            notes.append(f"Filtered `country`: {', '.join(selected[:6])}")

    if "shift" in out.columns:
        shifts = [str(v) for v in out["shift"].dropna().unique().tolist()]
        selected = [s for s in shifts if s.lower() in q]
        if selected:
            out = out[out["shift"].astype(str).isin(selected)]
            notes.append(f"Filtered `shift`: {', '.join(selected[:6])}")

    if "date" in out.columns and out["date"].notna().any():
        m_days = re.search(r"\blast\s+(\d+)\s+days?\b", q)
        if m_days:
            days = max(1, min(365, int(m_days.group(1))))
            max_date = out["date"].max()
            cutoff = max_date - pd.Timedelta(days=days)
            out = out[out["date"] >= cutoff]
            notes.append(f"Date filter: last {days} days")
        elif "last week" in q:
            max_date = out["date"].max()
            cutoff = max_date - pd.Timedelta(days=7)
            out = out[out["date"] >= cutoff]
            notes.append("Date filter: last 7 days")
        elif "last month" in q:
            max_date = out["date"].max()
            cutoff = max_date - pd.Timedelta(days=30)
            out = out[out["date"] >= cutoff]
            notes.append("Date filter: last 30 days")

    return out, notes, metric_override, top_n


def _store_ai_result(
    key_prefix: str,
    question: str,
    result_df: pd.DataFrame,
    sql_text: str,
    source: str,
    metric_override: str | None,
    chart_plan: dict[str, str] | None = None,
) -> None:
    now_text = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "question": question,
        "rows": int(len(result_df)),
        "sql": sql_text,
        "source": source,
        "timestamp": now_text,
        "metric_override": metric_override,
        "chart_plan": chart_plan or {},
        "result_df": result_df.copy(),
    }
    st.session_state[f"{key_prefix}_latest_result"] = payload

    history_key = f"{key_prefix}_history"
    history = st.session_state.get(history_key, [])
    history.insert(
        0,
        {
            "question": question,
            "rows": int(len(result_df)),
            "source": source,
            "timestamp": now_text,
        },
    )
    st.session_state[history_key] = history[:10]


def _render_latest_ai_result(key_prefix: str) -> None:
    payload = st.session_state.get(f"{key_prefix}_latest_result")
    if not payload:
        return

    latest_df = payload.get("result_df")
    if latest_df is None or not isinstance(latest_df, pd.DataFrame) or latest_df.empty:
        return

    st.markdown("**Latest AI Answer**")
    st.caption(
        f"{payload.get('timestamp', '')} | {payload.get('source', '')} | "
        f"{payload.get('rows', 0)} rows"
    )
    chart_plan = payload.get("chart_plan", {})
    if isinstance(chart_plan, dict) and chart_plan:
        _render_ai_plan_chart(
            latest_df,
            question=str(payload.get("question", "")),
            key_prefix=f"{key_prefix}_latest",
            ai_plan=chart_plan,
            metric_override=payload.get("metric_override"),
        )
    else:
        _render_question_driven_chart(
            latest_df,
            question=str(payload.get("question", "")),
            key_prefix=f"{key_prefix}_latest",
            metric_override=payload.get("metric_override"),
        )
    with st.expander("Latest Insights"):
        for line in _build_insights(
            latest_df,
            str(payload.get("question", "")),
            metric_override=payload.get("metric_override"),
        ):
            st.write(f"- {line}")
    with st.expander("Latest SQL"):
        st.code(str(payload.get("sql", "")), language="sql")


def _render_local_ai_fallback(
    fallback_df: pd.DataFrame,
    question: str,
    key_prefix: str,
) -> tuple[pd.DataFrame, str | None]:
    local_df, notes, metric_override, _ = _apply_question_filters(fallback_df, question)
    local_df = local_df.head(1000).copy()

    if local_df.empty:
        st.info("Fallback analysis found no matching rows for this question.")
        return local_df, metric_override

    st.success(f"Local fallback rows: {len(local_df)}")
    if notes:
        st.caption(" | ".join(notes))

    st.markdown("**Insights**")
    for line in _build_insights(local_df, question, metric_override=metric_override):
        st.write(f"- {line}")

    st.markdown("**Chart**")
    _render_question_driven_chart(
        local_df,
        question=question,
        key_prefix=f"{key_prefix}_local",
        metric_override=metric_override,
    )

    with st.expander("View Local Result Table"):
        st.dataframe(local_df, width='stretch')
    return local_df, metric_override


def _render_ask_results_modal(
    result_df: pd.DataFrame,
    question: str,
    final_sql: str,
    sql_source: str,
    schema: str,
    table: str,
    available_columns: list[str],
    sql_provider: str,
    model_override: str | None,
    key_prefix: str,
    ai_plan: dict[str, str] | None,
) -> None:
    """Render AI results in a dialog/modal window - chart first, insights on the side."""
    if result_df.empty:
        st.warning("⚠️ No data returned. Try a different question.")
        return

    metric_override = _pick_metric_from_question(question, list(result_df.columns))

    # Dialog title
    st.markdown(f"### 🤖 {question}")
    st.divider()

    # Main layout: Chart on left, Insights on right
    chart_col, insight_col = st.columns([2, 1], gap="medium")

    with chart_col:
        st.subheader("📈 Visualization")
        best_chart_type = _chart_intent_from_question(question)
        st.caption(f"Chart type: **{best_chart_type.title()}** (auto-selected)")

        try:
            if ai_plan:
                _render_ai_plan_chart(
                    result_df,
                    question=question,
                    key_prefix=key_prefix,
                    ai_plan=ai_plan,
                    metric_override=metric_override,
                )
            else:
                _render_question_driven_chart(
                    result_df,
                    question=question,
                    key_prefix=key_prefix,
                    metric_override=metric_override,
                )
        except Exception as chart_exc:
            st.warning(f"⚠️ Chart rendering failed: {chart_exc}")

    with insight_col:
        st.subheader("💡 Insights")
        insights = _build_insights(result_df, question, metric_override=metric_override)
        for insight in insights:
            st.write(f"• {insight}")

    st.divider()

    # Detailed explanation
    if st.checkbox("📖 Detailed Explanation", key=f"{key_prefix}_explanation"):
        with st.spinner("Generating detailed explanation..."):
            explanation = _generate_data_based_explanation(question, result_df)
            st.info(explanation)

    # Data table in expander
    with st.expander("📋 View Raw Data"):
        st.dataframe(result_df, width='stretch')

    # SQL in hidden expander
    with st.expander("🔧 Query Details (Advanced)"):
        st.markdown("**Generated SQL:**")
        st.code(final_sql, language="sql")
        st.caption(f"Data source: {sql_source}")

    _store_ai_result(
        key_prefix=key_prefix,
        question=question,
        result_df=result_df,
        sql_text=final_sql,
        source=sql_source,
        metric_override=metric_override,
        chart_plan=ai_plan,
    )


def _generate_llm_explanation(question: str, result_df: pd.DataFrame) -> str:
    """
    Generate a concise explanation (60-70 words) using LLM based on question and retrieved data.
    """
    if result_df.empty:
        return "No data available to analyze."
    
    try:
        model = os.getenv("OLLAMA_MODEL", "qwen3-coder-next").strip()
        base_url = os.getenv("OLLAMA_BASE_URL", "https://ollama.com").strip().rstrip("/")
        api_key = os.getenv("OLLAMA_API_KEY", "").strip()
        timeout_sec = float(os.getenv("OLLAMA_TIMEOUT_SEC", "30"))
        
        # Prepare data summary for LLM
        data_summary = f"Dataset: {len(result_df)} records\n"
        data_summary += f"Columns: {', '.join(result_df.columns[:5].tolist())}\n"
        
        # Add some sample statistics
        numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            col = numeric_cols[0]
            data_summary += f"\nSample metric ({col}): Min={result_df[col].min():.2f}, Max={result_df[col].max():.2f}, Avg={result_df[col].mean():.2f}"
        
        prompt = f"""You are a data analyst. Provide a concise explanation in exactly 60-70 words.

Question: {question}

Data Retrieved:
{data_summary}

Task: Provide a brief, clear explanation answering the question based on the data above. Keep it to 60-70 words maximum. Be specific and actionable."""
        
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
        }
        
        response = requests.post(
            f"{base_url}/api/chat",
            json=payload,
            headers=headers,
            timeout=timeout_sec,
        )
        
        if response.status_code == 200:
            response_data = response.json()
            explanation = response_data.get("message", {}).get("content", "").strip()
            if explanation:
                # Ensure it's within word limit
                words = explanation.split()
                if len(words) > 80:
                    explanation = " ".join(words[:70]) + "..."
                return explanation
        
        # Fallback if LLM fails
        return f"Unable to generate explanation. Retrieved {len(result_df)} records matching your query."
    
    except Exception as e:
        return f"Analysis: Retrieved {len(result_df)} records. Error generating explanation: {str(e)[:50]}"


def _generate_data_based_explanation(question: str, result_df: pd.DataFrame) -> str:
    """
    Generate meaningful explanation that actually answers the question based on data.
    Provides insights and conclusions, not just statistics.
    """
    if result_df.empty:
        return "❌ No data available to analyze."
    
    try:
        question_lower = question.lower()
        num_rows = len(result_df)
        columns = list(result_df.columns)
        numeric_cols = result_df.select_dtypes(include=['number']).columns.tolist()
        
        explanation = ""
        
        # ===== TREND ANALYSIS =====
        if any(word in question_lower for word in ["trend", "over time", "daily", "history", "change"]):
            explanation += "📈 **Trend Analysis:**\n\n"
            if 'date' in columns or 'time' in columns:
                date_col = next((c for c in columns if c.lower() in ['date', 'time']), None)
                if date_col and numeric_cols:
                    metric_col = numeric_cols[0]
                    try:
                        vals = result_df[metric_col].dropna()
                        if len(vals) > 1:
                            first_val = vals.iloc[0]
                            last_val = vals.iloc[-1]
                            change = ((last_val - first_val) / first_val * 100) if first_val != 0 else 0
                            direction = "📈 increasing" if change > 0 else "📉 decreasing"
                            explanation += f"• {metric_col} is {direction} by **{abs(change):.1f}%** over the period\n"
                            explanation += f"• Started at {first_val:.2f}, ended at {last_val:.2f}\n"
                    except:
                        pass
            if not explanation:
                explanation += f"• {num_rows} data points show trend over time\n"
            explanation += "\n"
        
        # ===== TOP PERFORMERS / RANKING =====
        elif any(word in question_lower for word in ["top", "best", "worst", "highest", "lowest", "rank", "compare"]):
            explanation += "🏆 **Rankings & Comparisons:**\n\n"
            if 'plant' in columns or any('plant' in c.lower() for c in columns):
                plant_col = next((c for c in columns if 'plant' in c.lower()), columns[0])
                if numeric_cols:
                    metric = numeric_cols[0]
                    try:
                        grouped = result_df.groupby(plant_col)[metric].agg(['mean', 'max', 'min', 'count'])
                        top = grouped.nlargest(3, 'mean')
                        explanation += f"**Top performers** (by avg {metric}):\n"
                        for idx, (plant, row) in enumerate(top.iterrows(), 1):
                            explanation += f"  {idx}. {plant}: Avg={row['mean']:.2f}, Max={row['max']:.2f}\n"
                        explanation += "\n"
                    except:
                        pass
            if not explanation.endswith("\n\n"):
                explanation += f"• Analysis includes {num_rows} records across multiple dimensions\n\n"
        
        # ===== COMPARISON / GAP ANALYSIS =====
        elif any(word in question_lower for word in ["gap", "difference", "vs", "compare", "between", "inconsistent"]):
            explanation += "⚖️ **Comparison & Gap Analysis:**\n\n"
            if numeric_cols:
                metrics = numeric_cols[:3]
                for metric in metrics:
                    try:
                        vals = result_df[metric].dropna()
                        if len(vals) > 0:
                            gap = vals.max() - vals.min()
                            avg = vals.mean()
                            gap_pct = (gap / avg * 100) if avg != 0 else 0
                            explanation += f"• **{metric}**: Gap of {gap:.2f} ({gap_pct:.1f}% of avg)\n"
                            explanation += f"  Range: {vals.min():.2f} - {vals.max():.2f}, Avg: {avg:.2f}\n"
                    except:
                        pass
            explanation += "\n"
        
        # ===== AGGREGATE / SUMMARY =====
        elif any(word in question_lower for word in ["total", "sum", "count", "aggregate", "show", "display"]):
            explanation += "📊 **Data Summary:**\n\n"
            if numeric_cols:
                for metric in numeric_cols[:3]:
                    try:
                        vals = result_df[metric].dropna()
                        explanation += f"• **{metric}**:\n"
                        explanation += f"  Total: {vals.sum():.2f}, Avg: {vals.mean():.2f}\n"
                        explanation += f"  Min: {vals.min():.2f}, Max: {vals.max():.2f}\n"
                    except:
                        pass
            explanation += "\n"
        
        # ===== DISTRIBUTION / VARIETY =====
        elif any(word in question_lower for word in ["distribution", "variety", "different", "spread", "variance"]):
            explanation += "🎯 **Distribution Analysis:**\n\n"
            if numeric_cols:
                metric = numeric_cols[0]
                try:
                    vals = result_df[metric].dropna()
                    explanation += f"• {metric} is distributed across {len(vals)} records\n"
                    explanation += f"  Unique values: {vals.nunique()}\n"
                    explanation += f"  Range: {vals.min():.2f} to {vals.max():.2f}\n"
                    explanation += f"  Standard deviation: {vals.std():.2f}\n"
                except:
                    pass
            explanation += "\n"
        
        # ===== DEFAULT / GENERAL ANALYSIS =====
        else:
            explanation += f"📋 **Data Overview for '{question}':**\n\n"
            explanation += f"• **Records analyzed**: {num_rows}\n"
            if numeric_cols:
                explanation += f"• **Key metrics**: {', '.join(numeric_cols[:3])}\n"
            if 'date' in columns or any('date' in c.lower() for c in columns):
                explanation += f"• **Time period**: Covers date range in data\n"
            if 'plant' in columns or any('plant' in c.lower() for c in columns):
                plant_col = next((c for c in columns if 'plant' in c.lower()), None)
                if plant_col:
                    unique_plants = result_df[plant_col].nunique()
                    explanation += f"• **Plants included**: {unique_plants} different locations\n"
            explanation += "\n"
        
        # ===== ADD KEY INSIGHT / CONCLUSION =====
        explanation += "**Key Insight:**\n"
        if len(result_df) > 100:
            explanation += f"• Large dataset ({num_rows} records) provides high confidence in patterns\n"
        elif len(result_df) > 20:
            explanation += f"• Medium dataset ({num_rows} records) shows clear trends\n"
        else:
            explanation += f"• Smaller dataset ({num_rows} records) - analyze with note on limited scope\n"
        
        # Add column hints for follow-up questions
        explanation += f"• Available fields: {', '.join(columns[:5])}" + ("..." if len(columns) > 5 else "") + "\n"
        
        return explanation.strip()
    
    except Exception as e:
        return f"📊 **Analysis Summary:**\n• {len(result_df)} records retrieved\n• Error: {str(e)}"


def _render_quick_ask_panel(
    schema: str,
    table: str,
    available_columns: list[str],
    sql_provider: str,
    fallback_df: pd.DataFrame | None = None,
) -> None:
    """
    Compact Quick Ask panel for top-right widget.
    Opens question in a modal dialog for detailed view.
    """
    st.caption("💬 Ask a question about your data")
    
    col_mapping = _build_column_mapping(available_columns)
    
    # Initialize session state
    if "quick_ask_question_for_dialog" not in st.session_state:
        st.session_state["quick_ask_question_for_dialog"] = ""
    if "show_quick_ask_dialog" not in st.session_state:
        st.session_state["show_quick_ask_dialog"] = False
    if "show_examples_dialog" not in st.session_state:
        st.session_state["show_examples_dialog"] = False
    if "quick_ask_input_text" not in st.session_state:
        st.session_state["quick_ask_input_text"] = ""
    if "should_clear_quick_ask_input" not in st.session_state:
        st.session_state["should_clear_quick_ask_input"] = False
    
    # Clear input on next render if flag is set (deferred clearing)
    if st.session_state.get("should_clear_quick_ask_input", False):
        st.session_state["quick_ask_input_text"] = ""
        st.session_state["should_clear_quick_ask_input"] = False
    
    col1, col2, col3 = st.columns([2.5, 0.75, 0.75])
    
    with col1:
        # Simple text input without form - properly handles session state
        question = st.text_input(
            "Question",
            key="quick_ask_input_text",
            placeholder="e.g., 'OEE by plant'",
            label_visibility="collapsed",
        )
    
    with col2:
        ask_clicked = st.button("🔍 Ask", use_container_width=True, key="quick_ask_button")
    
    with col3:
        examples_clicked = st.button("📌 Examples", use_container_width=True, key="quick_ask_examples_btn")
    
    # Direct action: If Ask button clicked, show dialog
    if ask_clicked and question.strip():
        st.session_state["quick_ask_question_for_dialog"] = question.strip()
        st.session_state["should_clear_quick_ask_input"] = True  # Clear on next render
        st.session_state["show_quick_ask_dialog"] = True
        st.rerun()  # Critical: Force rerun to display the dialog
    
    # Direct action: If examples clicked, show examples dialog
    if examples_clicked:
        st.session_state["show_quick_ask_dialog"] = False  # Explicitly close analysis dialog
        st.session_state["show_examples_dialog"] = True
        st.session_state["auto_refresh_enabled"] = False
        st.rerun()  # Force rerun to display examples dialog
    
    # Dialog 1: Analysis dialog
    @st.dialog("🤖 Quick Analysis", width="large")
    def analyze_question_dialog():
        user_question = st.session_state.get("quick_ask_question_for_dialog", "")
        
        # Close button
        if st.button("Close ✕", key="close_analysis_dialog", use_container_width=False):
            st.session_state["show_quick_ask_dialog"] = False
            st.session_state["quick_ask_question_for_dialog"] = ""  # Clear question
            st.session_state["should_clear_quick_ask_input"] = True  # Clear input on next render
            st.session_state["auto_refresh_enabled"] = True
            st.rerun()
        
        st.divider()
        
        if not user_question:
            st.warning("No question provided")
            return
        
        try:
            with st.spinner("🔄 Analyzing your question..."):
                ai_plan, model_name = _generate_ai_plan_via_ollama(
                    question=user_question,
                    schema=schema,
                    table=table,
                    available_columns=available_columns,
                )
                generated_sql = ai_plan.get("sql", "")
                validated_sql, warnings = _validate_and_fix_exasol_sql(
                    generated_sql,
                    schema=schema,
                    table=table,
                    available_columns=available_columns,
                )
                if warnings:
                    for warning in warnings:
                        st.warning(f"⚠️ {warning}")
                result_df = _run_query(_append_limit(validated_sql, row_limit=200))
            
            if not result_df.empty:
                st.markdown(f"### 📊 {user_question}")
                st.divider()
                
                col_chart, col_explanation = st.columns([1.2, 1], gap="large")
                
                with col_chart:
                    st.markdown("**📈 Chart**")
                    try:
                        _render_ai_plan_chart(
                            result_df,
                            question=user_question,
                            key_prefix="quick_ask_chart",
                            ai_plan=ai_plan,
                        )
                    except Exception as chart_exc:
                        st.warning(f"Chart rendering failed: {chart_exc}")
                
                with col_explanation:
                    st.markdown("**📖 Explanation**")
                    with st.spinner("⏳ Generating explanation..."):
                        explanation = _generate_llm_explanation(user_question, result_df)
                    st.markdown(explanation, unsafe_allow_html=True)
                
                st.divider()
                
                col1, col2 = st.columns(2)
                with col1:
                    with st.expander("📋 Show Data"):
                        st.dataframe(result_df, width='stretch')
                
                with col2:
                    with st.expander("📝 Show SQL"):
                        st.code(validated_sql, language="sql")
            else:
                st.warning("⚠️ No results returned. Try rephrasing your question.")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
    
    # Dialog 2: Examples dialog
    @st.dialog("📌 Example Questions", width="large")
    def examples_dialog():
        st.markdown("Click any example question to analyze:")
        example_questions = [
            "Show OEE trend",
            "Top 5 plants by OEE",
            "OEE vs calculated",
            "Production units trend",
            "Compare downtime across plants",
        ]
        
        col1, col2 = st.columns([1, 1])
        for idx, ex_q in enumerate(example_questions):
            col = col1 if idx % 2 == 0 else col2
            with col:
                if st.button(f"➜ {ex_q}", use_container_width=True, key=f"example_{ex_q}"):
                    # Populate the input field and close dialog
                    st.session_state["quick_ask_input_text"] = ex_q
                    st.session_state["show_examples_dialog"] = False
                    st.session_state["show_quick_ask_dialog"] = False  # Ensure analysis is closed
                    st.rerun()
        
        if st.button("Close", use_container_width=True, key="close_examples"):
            st.session_state["show_examples_dialog"] = False
            st.session_state["show_quick_ask_dialog"] = False  # Ensure analysis is closed
            st.rerun()
    
    # Show dialogs based on state
    if st.session_state.get("show_quick_ask_dialog"):
        analyze_question_dialog()
    elif st.session_state.get("show_examples_dialog"):
        examples_dialog()


def _render_ask_data(
    schema: str,
    table: str,
    available_columns: list[str],
    sql_provider: str = "ollama",
    model_override: str | None = None,
    compact: bool = False,
    key_prefix: str = "ask_data",
    fallback_df: pd.DataFrame | None = None,
) -> None:
    if compact:
        st.markdown("**🤖 Ask Data**")
        st.caption("Ask a question below or click an example")
    else:
        st.subheader("🤖 AI-Powered Analytics Assistant")
        st.caption("Ask business questions in plain English • Get instant SQL, insights, and visualizations")

    col_mapping = _build_column_mapping(available_columns)
    default_q = "Show OEE trend by plant over time"
    q_key = f"{key_prefix}_question"
    if q_key not in st.session_state:
        st.session_state[q_key] = default_q
    if f"{key_prefix}_last_executed" not in st.session_state:
        st.session_state[f"{key_prefix}_last_executed"] = ""

    # Input section with Help button
    input_col1, input_col2, input_col3 = st.columns([4.5, 0.5, 1])
    with input_col1:
        question = st.text_input("Your Question", key=q_key, placeholder="e.g., 'Show OEE gap by plant', 'What's our top performer?'")
    with input_col2:
        st.caption("")
        help_clicked = st.button("❓", key=f"{key_prefix}_help", width='stretch', help="See example questions and available data")
    with input_col3:
        st.caption("")
        asked = st.button("🔍 Ask", key=f"{key_prefix}_ask", width='stretch')

    # Check if question was just set from an example (for auto-execution)
    example_auto_trigger = False
    if q_key in st.session_state and st.session_state[q_key] != default_q:
        # Question differs from default - might be from example
        if f"{key_prefix}_last_executed" not in st.session_state or st.session_state[f"{key_prefix}_last_executed"] != st.session_state[q_key]:
            example_auto_trigger = True
            st.session_state[f"{key_prefix}_last_executed"] = st.session_state[q_key]

    # Dialog for examples and schema info
    if help_clicked:
        @st.dialog("📌 Example Questions & Available Data", width="large")
        def show_examples_and_schema():
            left_col, right_col = st.columns([1.2, 1], gap="large")
            
            with left_col:
                st.markdown("**📌 Example Questions:**")
                st.caption("Click any question to ask it")
                
                example_questions = [
                    "Show OEE trend by plant over time",
                    "What's the OEE gap between normalized and calculated?",
                    "Which plant has the best performance?",
                    "Show A/P/Q breakdown by plant",
                    "Top 5 plants by OEE",
                    "Data quality across plants",
                    "Show production units trend",
                    "Compare downtime across plants",
                    "Distribution of quality scores",
                    "Rank plants by availability",
                ]
                
                for ex_q in example_questions:
                    if st.button(f"📊 {ex_q}", width='stretch', key=f"example_{ex_q}"):
                        st.session_state[q_key] = ex_q
                        st.rerun()
            
            with right_col:
                st.markdown("**Database Schema:**")
                st.caption(f"`{schema}`.`{table}`")
                st.divider()
                
                st.markdown("**📋 Key Columns:**")
                col_descriptions = {
                    "date": "📅 Production date",
                    "plant_id": "🏭 Plant ID (P01-P12)",
                    "plant_name": "🏭 Plant name",
                    "oee_normalized": "⚡ OEE (A×P×Q)",
                    "availability": "📊 Availability",
                    "performance": "📊 Performance",
                    "quality": "📊 Quality",
                    "run_time_min": "⏱️ Runtime",
                    "downtime_min": "⏱️ Downtime",
                    "total_units": "📦 Units produced",
                    "good_units": "✅ Good units",
                    "data_source_type": "📡 Data source",
                }
                
                cols_display = [col for col in available_columns if col in col_descriptions]
                for col in cols_display:
                    st.caption(col_descriptions.get(col, col))
        
        show_examples_and_schema()

    if (asked or example_auto_trigger) and question:
        started = time.perf_counter()
        final_sql = ""
        sql_source = "heuristic"
        metric_override: str | None = None
        ai_plan: dict[str, str] | None = None
        result_df = pd.DataFrame()
        
        # Show processing status while generating
        status_placeholder = st.empty()
        
        try:
            with status_placeholder.container():
                status = st.status("🔄 Analyzing your question...", expanded=True)
            
                status.write("📝 Generating SQL query...")
                if sql_provider == "ollama":
                    try:
                        ai_plan, model_name = _generate_ai_plan_via_ollama(
                            question=question,
                            schema=schema,
                            table=table,
                            available_columns=available_columns,
                            model_override=model_override,
                        )
                        generated_sql = ai_plan.get("sql", "")
                        sql_source = f"ollama:{model_name}"
                        status.write(f"✅ Generated SQL via {sql_source}")
                    except Exception as model_exc:
                        error_msg = str(model_exc)
                        status.write(f"⚠️ Ollama error: {error_msg}")
                        status.write(f"🔧 Base URL: {os.getenv('OLLAMA_BASE_URL', 'http://127.0.0.1:11434')}")
                        status.write(f"🔧 Model: {os.getenv('OLLAMA_MODEL', 'qwen2.5-coder:7b')}")
                        status.write("🔄 Switching to heuristic fallback...")
                        generated_sql = _nl_to_sql_template(
                            question=question,
                            schema=schema,
                            table=table,
                            column_mapping=col_mapping,
                        )
                        sql_source = "heuristic-fallback"
                        ai_plan = None
                else:
                    generated_sql = _nl_to_sql_template(
                        question=question,
                        schema=schema,
                        table=table,
                        column_mapping=col_mapping,
                    )
                    sql_source = "heuristic"
                    ai_plan = None

                if not str(generated_sql).strip():
                    generated_sql = _nl_to_sql_template(
                        question=question,
                        schema=schema,
                        table=table,
                        column_mapping=col_mapping,
                    )
                    sql_source = f"{sql_source}-empty-fallback"

                status.write("🔧 Optimizing query...")
                generated_sql = _postprocess_generated_sql(
                    sql=generated_sql,
                    schema=schema,
                    table=table,
                    available_columns=available_columns,
                )
                
                # Validate and fix for Exasol compatibility
                generated_sql, exasol_warnings = _validate_and_fix_exasol_sql(
                    sql=generated_sql,
                    schema=schema,
                    table=table,
                    available_columns=available_columns,
                )
                for warning in exasol_warnings:
                    status.write(f"⚠️ {warning}")

                is_safe, detail = _ensure_safe_readonly_sql(generated_sql, table=table)
                if not is_safe:
                    status.update(label="❌ Query validation failed", state="error")
                    st.error(f"🚫 {detail}")
                    return

                final_sql = _append_limit(detail, row_limit=500)

                status.write("📊 Executing query...")
                try:
                    result_df = _run_query(final_sql)
                except Exception as first_exec_exc:
                    if sql_source.startswith("ollama"):
                        status.write("⚠️ Retrying with fallback...")
                        retry_sql = _nl_to_sql_template(
                            question=question,
                            schema=schema,
                            table=table,
                            column_mapping=col_mapping,
                        )
                        retry_sql = _postprocess_generated_sql(
                            sql=retry_sql,
                            schema=schema,
                            table=table,
                            available_columns=available_columns,
                        )
                        # Validate fallback SQL too
                        retry_sql, retry_warnings = _validate_and_fix_exasol_sql(
                            sql=retry_sql,
                            schema=schema,
                            table=table,
                            available_columns=available_columns,
                        )
                        for warning in retry_warnings:
                            status.write(f"⚠️ {warning}")
                        
                        retry_safe, retry_detail = _ensure_safe_readonly_sql(retry_sql, table=table)
                        if not retry_safe:
                            raise first_exec_exc

                        final_sql = _append_limit(retry_detail, row_limit=500)
                        result_df = _run_query(final_sql)
                        sql_source = "heuristic-retry"
                    else:
                        raise

                elapsed = time.perf_counter() - started
                status.update(label=f"✅ Complete ({elapsed:.1f}s)", state="complete")

            # Clear status and open modal with results
            status_placeholder.empty()
            
            if not result_df.empty:
                # Always open dialog for results (both compact and full modes)
                @st.dialog("🤖 AI Analysis Results", width="large")
                def show_results_modal():
                    _render_ask_results_modal(
                        result_df=result_df,
                        question=question,
                        final_sql=final_sql,
                        sql_source=sql_source,
                        schema=schema,
                        table=table,
                        available_columns=available_columns,
                        sql_provider=sql_provider,
                        model_override=model_override,
                        key_prefix=key_prefix,
                        ai_plan=ai_plan,
                    )
                show_results_modal()
            else:
                st.warning("⚠️ No data returned. Try a different question.")
                
        except Exception as exc:
            status_placeholder.empty()
            elapsed = time.perf_counter() - started
            st.error(f"🔴 Error ({elapsed:.1f}s): {exc}")


def _render_latest_ai_result(key_prefix: str) -> None:
    payload = st.session_state.get(f"{key_prefix}_latest_result")
    if not payload:
        return

    latest_df = payload.get("result_df")
    if latest_df is None or not isinstance(latest_df, pd.DataFrame) or latest_df.empty:
        return

    st.markdown("**Previously Asked**")
    st.caption(
        f"{payload.get('timestamp', '')} | {payload.get('source', '')} | "
        f"{payload.get('rows', 0)} rows"
    )
    
    # Show question
    st.markdown(f"_Question: {payload.get('question', '')}_")
    
    chart_plan = payload.get("chart_plan", {})
    if isinstance(chart_plan, dict) and chart_plan:
        _render_ai_plan_chart(
            latest_df,
            question=str(payload.get("question", "")),
            key_prefix=f"{key_prefix}_latest",
            ai_plan=chart_plan,
            metric_override=payload.get("metric_override"),
        )
    else:
        _render_question_driven_chart(
            latest_df,
            question=str(payload.get("question", "")),
            key_prefix=f"{key_prefix}_latest",
            metric_override=payload.get("metric_override"),
        )


@st.cache_data(ttl=10)
def load_unified_data(
    schema: str,
    table: str,
    limit: int,
    refresh_key: str,
) -> tuple[pd.DataFrame, list[str]]:
    conn = _connect()
    try:
        actual_schema, actual_table, available_columns = _discover_table(conn, schema, table)
        from_table = f"{_quote_ident(actual_schema)}.{_quote_ident(actual_table)}"
        sql = f"SELECT * FROM {from_table} LIMIT {int(limit)}"

        stmt = conn.execute(sql)
        rows = stmt.fetchall()
        columns = _extract_columns(stmt)
        df = pd.DataFrame(rows, columns=columns)
        rename_map = _canonical_column_rename_map(columns)
        if rename_map:
            df = df.rename(columns=rename_map)
        return df, list(df.columns)
    finally:
        conn.close()


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    numeric_cols = [
        "planned_time_min",
        "run_time_min",
        "downtime_min",
        "total_units",
        "good_units",
        "defective_units",
        "availability",
        "performance",
        "quality",
        "oee_reported",
        "oee_normalized",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    text_like = {"plant_id", "plant_name", "country", "shift", "data_source_type"}
    for col in out.columns:
        if col in text_like or col in numeric_cols:
            continue
        if pd.api.types.is_numeric_dtype(out[col]):
            continue
        conv = pd.to_numeric(out[col], errors="coerce")
        original_non_null = out[col].notna().sum()
        if original_non_null == 0:
            continue
        valid_ratio = float(conv.notna().sum()) / float(original_non_null)
        if valid_ratio >= 0.6:
            out[col] = conv

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
    else:
        for candidate in ["event_date", "timestamp", "event_time", "day"]:
            if candidate in out.columns:
                parsed = pd.to_datetime(out[candidate], errors="coerce")
                if parsed.notna().sum() >= max(3, len(out) // 5):
                    out["date"] = parsed
                    break

    if "plant_name" not in out.columns and "plant_id" in out.columns:
        out["plant_name"] = out["plant_id"].astype(str)
    elif "plant_name" in out.columns:
        out["plant_name"] = out["plant_name"].fillna("").astype(str)
        if "plant_id" in out.columns:
            out.loc[out["plant_name"] == "", "plant_name"] = out["plant_id"].astype(str)

    return out


def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    # Sidebar filters were removed; charts and KPI cards now own local filters.
    return df.copy()


def _render_comprehensive_kpi_dashboard(df: pd.DataFrame) -> None:
    """
    Comprehensive KPI dashboard with real-time metrics, trends, and comparisons.
    """
    st.subheader("📊 Comprehensive KPI Dashboard")
    
    # KPI Controls (filters)
    filter_cols = st.columns([1.4, 1, 1], gap="large")
    scope_df = df.copy()
    
    with filter_cols[0]:
        if "date" in scope_df.columns and scope_df["date"].notna().any():
            scope_df["date"] = pd.to_datetime(scope_df["date"], errors="coerce")
            max_date = scope_df["date"].max()
            days = st.slider(
                "📅 Time Window (days)",
                min_value=1,
                max_value=180,
                value=30,
                key="kpi_window_days",
            )
            cutoff = max_date - pd.Timedelta(days=int(days))
            scope_df = scope_df[scope_df["date"] >= cutoff]
        else:
            st.caption("Date column unavailable; using full range.")

    with filter_cols[1]:
        if "data_source_type" in scope_df.columns:
            src_values = sorted([str(v) for v in scope_df["data_source_type"].dropna().unique().tolist()])
            sel_src = st.multiselect(
                "🔍 Source Type",
                src_values,
                default=src_values,
                key="kpi_source_filter",
            )
            if sel_src:
                scope_df = scope_df[scope_df["data_source_type"].astype(str).isin(sel_src)]

    with filter_cols[2]:
        plant_col = _plant_column(scope_df)
        if plant_col and plant_col in scope_df.columns:
            plant_values = sorted([str(v) for v in scope_df[plant_col].dropna().unique().tolist()])
            default = plant_values[: min(10, len(plant_values))]
            sel_plants = st.multiselect(
                "🏭 Plants",
                plant_values,
                default=default,
                key="kpi_plant_filter",
            )
            if sel_plants:
                scope_df = scope_df[scope_df[plant_col].astype(str).isin(sel_plants)]

    st.divider()
    
    # Main KPI Cards (Row 1)
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4, gap="medium")

    plant_col = _plant_column(scope_df)
    
    # Active Plants
    with kpi_col1:
        with st.container(border=True):
            st.markdown("🏭 **Active Plants**")
            plant_count = scope_df[plant_col].nunique() if plant_col and plant_col in scope_df.columns else 0
            st.markdown(f"# {plant_count}")
            if len(scope_df) > 0:
                st.caption(f"📊 {len(scope_df):,} records")

    # Average OEE
    with kpi_col2:
        with st.container(border=True):
            st.markdown("📊 **Avg OEE**")
            if "oee_normalized" in scope_df.columns and scope_df["oee_normalized"].notna().any():
                oee_val = pd.to_numeric(scope_df["oee_normalized"], errors="coerce").mean()
                st.markdown(f"# {oee_val*100:.1f}%")
                baseline = 0.75
                trend = "↑" if oee_val > baseline else "↓"
                st.caption(f"{trend} vs {baseline*100:.0f}% baseline")
            else:
                st.markdown("# N/A")

    # Yield Rate
    with kpi_col3:
        with st.container(border=True):
            st.markdown("📦 **Yield Rate**")
            if {"good_units", "total_units"}.issubset(scope_df.columns):
                good = pd.to_numeric(scope_df["good_units"], errors="coerce").sum()
                total = pd.to_numeric(scope_df["total_units"], errors="coerce").sum()
                if total > 0:
                    yield_rate = (good / total) * 100
                    st.markdown(f"# {yield_rate:.1f}%")
                    trend = "↑" if yield_rate > 95 else "↓"
                    st.caption(f"{trend} Good: {good:,.0f} / {total:,.0f}")
                else:
                    st.markdown("# N/A")
            else:
                st.markdown("# N/A")

    # Downtime Summary
    with kpi_col4:
        with st.container(border=True):
            st.markdown("⏸️ **Avg Downtime**")
            if "downtime_min" in scope_df.columns and scope_df["downtime_min"].notna().any():
                downtime_avg = pd.to_numeric(scope_df["downtime_min"], errors="coerce").mean()
                st.markdown(f"# {downtime_avg:.1f}m")
                st.caption(f"📈 Total: {pd.to_numeric(scope_df['downtime_min'], errors='coerce').sum():,.0f}m")
            else:
                st.markdown("# N/A")

    st.divider()
    
    # Secondary Metrics (Row 2)
    sec_col1, sec_col2, sec_col3 = st.columns(3, gap="medium")

    # Availability
    with sec_col1:
        with st.container(border=True):
            st.markdown("🟢 **Availability**")
            if "availability" in scope_df.columns and scope_df["availability"].notna().any():
                avail = pd.to_numeric(scope_df["availability"], errors="coerce").mean() * 100
                st.markdown(f"# {avail:.1f}%")
                st.caption("A component of OEE")
            else:
                st.markdown("# N/A")

    # Performance
    with sec_col2:
        with st.container(border=True):
            st.markdown("⚡ **Performance**")
            if "performance" in scope_df.columns and scope_df["performance"].notna().any():
                perf = pd.to_numeric(scope_df["performance"], errors="coerce").mean() * 100
                st.markdown(f"# {perf:.1f}%")
                st.caption("P component of OEE")
            else:
                st.markdown("# N/A")

    # Quality
    with sec_col3:
        with st.container(border=True):
            st.markdown("✅ **Quality**")
            if "quality" in scope_df.columns and scope_df["quality"].notna().any():
                qual = pd.to_numeric(scope_df["quality"], errors="coerce").mean() * 100
                st.markdown(f"# {qual:.1f}%")
                st.caption("Q component of OEE")
            else:
                st.markdown("# N/A")


def _render_metrics(df: pd.DataFrame) -> None:
    st.subheader("📈 Realtime KPI Cards")
    scope_df = df.copy()

    filter_cols = st.columns([1.4, 1, 1], gap="large")
    with filter_cols[0]:
        if "date" in scope_df.columns and scope_df["date"].notna().any():
            max_date = scope_df["date"].max()
            days = st.slider(
                "📅 KPI Time Window (days)",
                min_value=1,
                max_value=180,
                value=30,
                key="kpi_window_days",
            )
            cutoff = max_date - pd.Timedelta(days=int(days))
            scope_df = scope_df[scope_df["date"] >= cutoff]
        else:
            st.caption("📌 Date column unavailable; using full range.")

    with filter_cols[1]:
        if "data_source_type" in scope_df.columns:
            src_values = sorted(
                [str(v) for v in scope_df["data_source_type"].dropna().unique().tolist()]
            )
            sel_src = st.multiselect(
                "🔍 KPI Source Type",
                src_values,
                default=src_values,
                key="kpi_source_filter",
            )
            if sel_src:
                scope_df = scope_df[
                    scope_df["data_source_type"].astype(str).isin(sel_src)
                ]

    with filter_cols[2]:
        plant_col = _plant_column(scope_df)
        if plant_col and plant_col in scope_df.columns:
            plant_values = sorted(
                [str(v) for v in scope_df[plant_col].dropna().unique().tolist()]
            )
            default = plant_values[: min(10, len(plant_values))]
            sel_plants = st.multiselect(
                "🏭 KPI Plants",
                plant_values,
                default=default,
                key="kpi_plant_filter",
            )
            if sel_plants:
                scope_df = scope_df[scope_df[plant_col].astype(str).isin(sel_plants)]

    # KPI Cards with better styling
    c1, c2, c3, c4 = st.columns(4)

    plant_col = _plant_column(scope_df)
    if plant_col and plant_col in scope_df.columns:
        c1.metric("🏭 Active Plants", f"{scope_df[plant_col].nunique():,}")
    else:
        c1.metric("🏭 Active Plants", "N/A")

    if "oee_normalized" in scope_df.columns and scope_df["oee_normalized"].notna().any():
        oee_val = scope_df['oee_normalized'].mean()
        delta_val = (oee_val - 0.75) * 100 if oee_val >= 0.75 else None
        c2.metric("📊 Avg OEE (Normalized)", f"{oee_val * 100:.2f}%", delta=delta_val)
    else:
        c2.metric("📊 Avg OEE (Normalized)", "N/A")

    if {"good_units", "total_units"}.issubset(scope_df.columns):
        good = pd.to_numeric(scope_df["good_units"], errors="coerce").sum()
        total = pd.to_numeric(scope_df["total_units"], errors="coerce").sum()
        if total > 0:
            yield_rate = (good / total) * 100
            delta_val = yield_rate - 95 if yield_rate >= 95 else None
            c3.metric("📦 Yield Rate", f"{yield_rate:.2f}%", delta=delta_val)
        else:
            c3.metric("📦 Yield Rate", "N/A")
    else:
        c3.metric("📦 Yield Rate", "N/A")

    if "downtime_min" in scope_df.columns and scope_df["downtime_min"].notna().any():
        downtime_avg = pd.to_numeric(scope_df['downtime_min'], errors='coerce').mean()
        c4.metric("⏸️ Avg Downtime (min)", f"{downtime_avg:.2f}")
    elif "performance" in scope_df.columns and scope_df["performance"].notna().any():
        perf = scope_df['performance'].mean() * 100
        c4.metric("⚡ Avg Performance", f"{perf:.2f}%")
    else:
        c4.metric("⚡ Avg Performance", "N/A")


def _render_basic_line_chart(df: pd.DataFrame) -> None:
    st.subheader("📊 Operations Visuals")
    numeric_cols = [
        c for c in df.select_dtypes(include=["number"]).columns if "id" not in c.lower()
    ]
    if not numeric_cols:
        st.info("No numeric columns available for charts.")
        return

    categorical_cols = [
        c
        for c in df.columns
        if (pd.api.types.is_object_dtype(df[c]) or str(df[c].dtype).startswith("string"))
        and df[c].nunique(dropna=True) <= 60
    ]
    date_col = _pick_date_column(df)
    plant_col = _plant_column(df)

    tab_trend, tab_compare, tab_comp = st.tabs(
        ["📈 Trend Explorer", "🔍 Comparison Explorer", "🥧 Composition Explorer"]
    )

    with tab_trend:
        work = df.copy()
        ctrl = st.columns([1.2, 1, 1], gap="large")
        with ctrl[0]:
            trend_metric = st.selectbox(
                "Trend Metric",
                options=numeric_cols,
                index=numeric_cols.index("oee_normalized")
                if "oee_normalized" in numeric_cols
                else 0,
                key="trend_metric_select",
            )
        with ctrl[1]:
            if "data_source_type" in work.columns:
                src = sorted([str(v) for v in work["data_source_type"].dropna().unique().tolist()])
                selected_src = st.multiselect(
                    "Trend Source",
                    src,
                    default=src,
                    key="trend_src_filter",
                )
                if selected_src:
                    work = work[work["data_source_type"].astype(str).isin(selected_src)]
        with ctrl[2]:
            if plant_col and plant_col in work.columns:
                plants = sorted([str(v) for v in work[plant_col].dropna().unique().tolist()])
                selected_plants = st.multiselect(
                    "Trend Plants",
                    plants,
                    default=plants[: min(8, len(plants))],
                    key="trend_plant_filter",
                )
                if selected_plants:
                    work = work[work[plant_col].astype(str).isin(selected_plants)]

        if date_col:
            valid_date = pd.to_datetime(work[date_col], errors="coerce").dropna()
            if not valid_date.empty:
                min_d = valid_date.min().date()
                max_d = valid_date.max().date()
                dr = st.date_input(
                    "Trend Date Range",
                    value=(min_d, max_d),
                    min_value=min_d,
                    max_value=max_d,
                    key="trend_date_range",
                )
                work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
                work = work[
                    work[date_col].between(pd.to_datetime(dr[0]), pd.to_datetime(dr[1]))
                ]

        if not date_col:
            st.info("No date/time column found for trend analysis.")
        else:
            work["__metric"] = pd.to_numeric(work[trend_metric], errors="coerce")
            work = work.dropna(subset=[date_col, "__metric"])
            if work.empty:
                st.info("No valid rows for selected trend filters.")
            else:
                if plant_col and plant_col in work.columns and work[plant_col].nunique(dropna=True) <= 12:
                    chart_df = work.groupby([date_col, plant_col], as_index=False)["__metric"].mean()
                    chart = (
                        alt.Chart(chart_df)
                        .mark_line(point=True, strokeWidth=2.4)
                        .encode(
                            x=alt.X(f"{date_col}:T", title="Date"),
                            y=alt.Y("__metric:Q", title=trend_metric),
                            color=alt.Color(f"{plant_col}:N", title="Plant"),
                            tooltip=[
                                alt.Tooltip(f"{date_col}:T", title="Date"),
                                alt.Tooltip(f"{plant_col}:N", title="Plant"),
                                alt.Tooltip("__metric:Q", title=trend_metric, format=".4f"),
                            ],
                        )
                        .properties(height=340)
                        .interactive()
                    )
                else:
                    chart_df = work.groupby(date_col, as_index=False)["__metric"].mean()
                    chart = (
                        alt.Chart(chart_df)
                        .mark_line(point=True, strokeWidth=2.4, color="#0f766e")
                        .encode(
                            x=alt.X(f"{date_col}:T", title="Date"),
                            y=alt.Y("__metric:Q", title=trend_metric),
                            tooltip=[
                                alt.Tooltip(f"{date_col}:T", title="Date"),
                                alt.Tooltip("__metric:Q", title=trend_metric, format=".4f"),
                            ],
                        )
                        .properties(height=340)
                        .interactive()
                    )
                st.altair_chart(chart, width='stretch')

    with tab_compare:
        work = df.copy()
        dim_options = categorical_cols or ([plant_col] if plant_col else [])
        if not dim_options:
            st.info("No categorical dimensions available for comparison.")
        else:
            ctrl = st.columns([1, 1, 1, 1], gap="large")
            with ctrl[0]:
                compare_dim = st.selectbox(
                    "Compare By",
                    options=dim_options,
                    index=dim_options.index(plant_col)
                    if plant_col and plant_col in dim_options
                    else 0,
                    key="compare_dim_select",
                )
            with ctrl[1]:
                compare_metric = st.selectbox(
                    "Compare Metric",
                    options=numeric_cols,
                    index=numeric_cols.index("oee_normalized")
                    if "oee_normalized" in numeric_cols
                    else 0,
                    key="compare_metric_select",
                )
            with ctrl[2]:
                agg_mode = st.selectbox(
                    "Aggregation",
                    options=["mean", "sum", "median"],
                    index=0,
                    key="compare_agg_select",
                )
            with ctrl[3]:
                ranking_mode = st.selectbox(
                    "Ranking",
                    options=["top", "bottom"],
                    index=0,
                    key="compare_rank_select",
                )

            top_n = st.slider("Compare Top N", 3, 30, 12, 1, key="compare_topn_slider")
            work["__metric"] = pd.to_numeric(work[compare_metric], errors="coerce")
            grouped = work.dropna(subset=[compare_dim, "__metric"]).groupby(compare_dim, as_index=False)["__metric"]
            if agg_mode == "sum":
                comp_df = grouped.sum()
            elif agg_mode == "median":
                comp_df = grouped.median()
            else:
                comp_df = grouped.mean()

            comp_df = comp_df.sort_values("__metric", ascending=(ranking_mode == "bottom")).head(top_n)
            if comp_df.empty:
                st.info("No valid rows for comparison filters.")
            else:
                chart = (
                    alt.Chart(comp_df)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X(f"{compare_dim}:N", sort=comp_df[compare_dim].tolist(), title=compare_dim),
                        y=alt.Y("__metric:Q", title=f"{agg_mode}({compare_metric})"),
                        color=alt.Color("__metric:Q", scale=alt.Scale(scheme="teals"), title="Value"),
                        tooltip=[
                            alt.Tooltip(f"{compare_dim}:N", title=compare_dim),
                            alt.Tooltip("__metric:Q", title=f"{agg_mode}({compare_metric})", format=".4f"),
                        ],
                    )
                    .properties(height=340)
                )
                st.altair_chart(chart, width='stretch')

    with tab_comp:
        work = df.copy()
        dim_options = categorical_cols.copy()
        if "data_source_type" in dim_options:
            dim_options.insert(0, dim_options.pop(dim_options.index("data_source_type")))
        if not dim_options:
            st.info("No categorical dimensions available for composition.")
        else:
            ctrl = st.columns([1, 1, 1], gap="large")
            with ctrl[0]:
                pie_dim = st.selectbox(
                    "Composition Dimension",
                    options=dim_options,
                    index=0,
                    key="comp_dim_select",
                )
            with ctrl[1]:
                value_mode = st.selectbox(
                    "Composition Value",
                    options=["row_count", *numeric_cols],
                    index=0,
                    key="comp_value_mode_select",
                )
            with ctrl[2]:
                limit = st.slider("Max Segments", 3, 15, 8, 1, key="comp_limit_slider")

            if value_mode == "row_count":
                comp_df = (
                    work.dropna(subset=[pie_dim])
                    .groupby(pie_dim, as_index=False)
                    .size()
                    .rename(columns={"size": "__value"})
                )
            else:
                work["__value"] = pd.to_numeric(work[value_mode], errors="coerce")
                comp_df = (
                    work.dropna(subset=[pie_dim, "__value"])
                    .groupby(pie_dim, as_index=False)["__value"]
                    .sum()
                )

            comp_df = comp_df.sort_values("__value", ascending=False).head(limit)
            if comp_df.empty:
                st.info("No valid rows for composition filters.")
            else:
                pie = (
                    alt.Chart(comp_df)
                    .mark_arc(innerRadius=65)
                    .encode(
                        theta=alt.Theta("__value:Q", title=value_mode),
                        color=alt.Color(f"{pie_dim}:N", title=pie_dim),
                        tooltip=[
                            alt.Tooltip(f"{pie_dim}:N", title=pie_dim),
                            alt.Tooltip("__value:Q", title=value_mode, format=".4f"),
                        ],
                    )
                    .properties(height=340)
                )
                st.altair_chart(pie, width='stretch')


def _render_oee_gap_analysis(df: pd.DataFrame) -> None:
    """Highlight the key demo point: OEE normalization reveals performance gaps"""
    st.subheader("🔍 OEE Normalization Gap Analysis")
    st.caption("🎯 Key Finding: Reported vs Calculated OEE reveals hidden performance gaps across MES types")
    
    if not {"availability", "performance", "quality", "oee_normalized"}.issubset(df.columns):
        st.info("Missing required columns for OEE gap analysis")
        return
    
    work = df.copy()
    work["availability"] = pd.to_numeric(work["availability"], errors="coerce")
    work["performance"] = pd.to_numeric(work["performance"], errors="coerce")
    work["quality"] = pd.to_numeric(work["quality"], errors="coerce")
    work["oee_normalized"] = pd.to_numeric(work["oee_normalized"], errors="coerce")
    
    # Calculate theoretical OEE (A×P×Q)
    work["oee_calculated"] = work["availability"] * work["performance"] * work["quality"]
    work["oee_gap"] = (work["oee_normalized"] - work["oee_calculated"]).abs()
    
    work = work.dropna(subset=["oee_normalized", "oee_calculated"])
    if work.empty:
        st.info("No valid OEE data for gap analysis")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    avg_gap = work["oee_gap"].mean()
    max_gap = work["oee_gap"].max()
    gap_rows = (work["oee_gap"] > 0.05).sum()
    gap_pct = 100.0 * gap_rows / len(work) if len(work) > 0 else 0
    
    col1.metric("⚠️ Avg OEE Gap", f"{avg_gap:.4f}")
    col2.metric("🔴 Max OEE Gap", f"{max_gap:.4f}")
    col3.metric("❌ Problematic Records", f"{gap_rows:,}")
    col4.metric("🚨 % with Gap > 5%", f"{gap_pct:.1f}%")
    
    # Chart: OEE Comparison
    plant_col = _plant_column(work)
    if plant_col and plant_col in work.columns:
        comparison_df = work.groupby(plant_col, as_index=False)[["oee_normalized", "oee_calculated"]].mean()
        comparison_df_melted = comparison_df.melt(id_vars=[plant_col], var_name="oee_type", value_name="oee_value")
        comparison_df_melted["oee_type"] = comparison_df_melted["oee_type"].map(
            {"oee_normalized": "Normalized (Actual)", "oee_calculated": "Calculated (A×P×Q)"}
        )
        
        chart = (
            alt.Chart(comparison_df_melted)
            .mark_bar(opacity=0.8)
            .encode(
                x=alt.X(f"{plant_col}:N", title="Plant"),
                y=alt.Y("oee_value:Q", title="OEE", axis=alt.Axis(format=".0%")),
                color=alt.Color("oee_type:N", title="OEE Type", scale=alt.Scale(scheme="set2")),
                tooltip=[
                    alt.Tooltip(f"{plant_col}:N", title="Plant"),
                    alt.Tooltip("oee_type:N", title="Type"),
                    alt.Tooltip("oee_value:Q", title="OEE", format=".3f"),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, width='stretch')
    
    # Insight box
    with st.container(border=True):
        st.markdown("### 💡 Insight")
        if avg_gap > 0.1:
            st.warning(
                f"🚨 **High OEE Inconsistency Detected**: Average gap is {avg_gap:.1%}. "
                f"This indicates that reported OEE values differ significantly from the calculated (A×P×Q) formula. "
                f"Investigate:\n"
                f"- Different OEE calculation methods across MES systems\n"
                f"- Data entry errors or unit mismatches\n"
                f"- Missing or incorrect availability/performance/quality inputs"
            )
        elif avg_gap > 0.05:
            st.info(
                f"⚠️ **Moderate OEE Variance**: Average gap is {avg_gap:.1%}. "
                f"Some plants may have custom OEE formulas or data quality issues."
            )
        else:
            st.success(
                f"✅ **OEE Consistency Good**: Average gap is {avg_gap:.1%}. "
                f"Normalized OEE aligns well with calculated (A×P×Q) formula."
            )


def _render_mes_data_quality_dashboard(df: pd.DataFrame) -> None:
    """Show data quality and completeness by MES type"""
    st.subheader("📊 MES Data Quality Dashboard")
    st.caption("🔍 Assess data completeness and identify challenges by MES source type")
    
    if "data_source_type" not in df.columns:
        st.info("No data_source_type column available")
        return
    
    work = df.copy()
    source_col = "data_source_type"
    
    # Data quality per source type
    quality_metrics = []
    for source, group in work.groupby(source_col, dropna=False):
        metrics = {
            "Source Type": str(source),
            "Row Count": len(group),
            "OEE Complete": (group["oee_normalized"].notna().sum() / len(group) * 100) if len(group) > 0 else 0,
            "Availability": (group["availability"].notna().sum() / len(group) * 100) if len(group) > 0 else 0,
            "Performance": (group["performance"].notna().sum() / len(group) * 100) if len(group) > 0 else 0,
            "Quality": (group["quality"].notna().sum() / len(group) * 100) if len(group) > 0 else 0,
            "Downtime": (group["downtime_min"].notna().sum() / len(group) * 100) if len(group) > 0 else 0,
            "Units": (group["total_units"].notna().sum() / len(group) * 100) if len(group) > 0 else 0,
        }
        quality_metrics.append(metrics)
    
    quality_df = pd.DataFrame(quality_metrics)
    
    # Show as heatmap
    completeness_cols = ["OEE Complete", "Availability", "Performance", "Quality", "Downtime", "Units"]
    heatmap_data = quality_df.set_index("Source Type")[completeness_cols]
    
    col1, col2 = st.columns([2, 1])
    with col1:
        st.caption("📈 Data Completeness by MES Type (%)")
        heatmap_chart = (
            alt.Chart(quality_df.melt(id_vars="Source Type", value_vars=completeness_cols, var_name="Field", value_name="Completeness"))
            .mark_rect()
            .encode(
                x=alt.X("Field:N", title="Field"),
                y=alt.Y("Source Type:N", title="MES Source Type"),
                color=alt.Color("Completeness:Q", scale=alt.Scale(scheme="greens"), title="Completeness %"),
                tooltip=[
                    alt.Tooltip("Source Type:N", title="Source"),
                    alt.Tooltip("Field:N", title="Field"),
                    alt.Tooltip("Completeness:Q", title="Completeness %", format=".1f"),
                ],
            )
            .properties(height=250)
        )
        st.altair_chart(heatmap_chart, width='stretch')
    
    with col2:
        st.caption("📋 Challenges by MES Type")
        for _, row in quality_df.iterrows():
            source = row["Source Type"]
            st.markdown(f"**{source}**")
            missing = []
            if row["Downtime"] < 80:
                missing.append("⚠️ Downtime")
            if row["Availability"] < 80:
                missing.append("⚠️ Availability")
            if row["Performance"] < 80:
                missing.append("⚠️ Performance")
            if row["Units"] < 80:
                missing.append("⚠️ Units")
            if missing:
                st.markdown(" | ".join(missing))
            else:
                st.markdown("✅ Complete")


def _render_normalization_problem_solver(df: pd.DataFrame) -> None:
    st.subheader("🔬 Normalization Problem Solver")
    st.caption(
        "🔍 Detect schema gaps, unit mismatches, and KPI inconsistencies across MES source types."
    )

    work = df.copy()
    plant_col = _plant_column(work)
    source_col = "data_source_type" if "data_source_type" in work.columns else None

    controls = st.columns([1, 1, 1], gap="large")
    with controls[0]:
        if source_col:
            src_values = sorted([str(v) for v in work[source_col].dropna().unique().tolist()])
            sel_src = st.multiselect(
                "Solver Source Filter",
                src_values,
                default=src_values,
                key="solver_source_filter",
            )
            if sel_src:
                work = work[work[source_col].astype(str).isin(sel_src)]
    with controls[1]:
        if plant_col and plant_col in work.columns:
            plant_values = sorted([str(v) for v in work[plant_col].dropna().unique().tolist()])
            sel_plants = st.multiselect(
                "Solver Plant Filter",
                plant_values,
                default=plant_values[: min(10, len(plant_values))],
                key="solver_plant_filter",
            )
            if sel_plants:
                work = work[work[plant_col].astype(str).isin(sel_plants)]
    with controls[2]:
        if "date" in work.columns and pd.to_datetime(work["date"], errors="coerce").notna().any():
            date_series = pd.to_datetime(work["date"], errors="coerce").dropna()
            min_d = date_series.min().date()
            max_d = date_series.max().date()
            dr = st.date_input(
                "Solver Date Range",
                value=(min_d, max_d),
                min_value=min_d,
                max_value=max_d,
                key="solver_date_range",
            )
            work["date"] = pd.to_datetime(work["date"], errors="coerce")
            work = work[work["date"].between(pd.to_datetime(dr[0]), pd.to_datetime(dr[1]))]

    row_count = len(work)
    if row_count == 0:
        st.info("No rows available for diagnostics.")
        return

    critical_cols = [
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
    present_critical = [c for c in critical_cols if c in work.columns]

    c1, c2, c3, c4 = st.columns(4)
    if plant_col and plant_col in work.columns:
        c1.metric("Plants Covered", f"{work[plant_col].nunique():,}")
    elif source_col and source_col in work.columns:
        c1.metric("Source Types Covered", f"{work[source_col].nunique():,}")
    else:
        c1.metric("Completeness Scope", "Filtered")

    if present_critical:
        complete_rate = float(work[present_critical].notna().all(axis=1).mean())
        c2.metric("Critical Completeness", f"{complete_rate * 100:.1f}%")
    else:
        complete_rate = 0.0
        c2.metric("Critical Completeness", "N/A")

    reported_gap_mean = None
    if {"oee_reported", "oee_normalized"}.issubset(work.columns):
        gap = (pd.to_numeric(work["oee_reported"], errors="coerce") - pd.to_numeric(work["oee_normalized"], errors="coerce")).abs()
        if gap.notna().any():
            reported_gap_mean = float(gap.mean())
            c3.metric("Avg Reported vs Normalized Gap", f"{reported_gap_mean * 100:.2f} pp")
        else:
            c3.metric("Avg Reported vs Normalized Gap", "N/A")
    else:
        c3.metric("Avg Reported vs Normalized Gap", "N/A")

    apq_gap_mean = None
    if {"availability", "performance", "quality", "oee_normalized"}.issubset(work.columns):
        calc = (
            pd.to_numeric(work["availability"], errors="coerce")
            * pd.to_numeric(work["performance"], errors="coerce")
            * pd.to_numeric(work["quality"], errors="coerce")
        )
        apq_gap = (calc - pd.to_numeric(work["oee_normalized"], errors="coerce")).abs()
        if apq_gap.notna().any():
            apq_gap_mean = float(apq_gap.mean())
            c4.metric("Avg APQ vs Normalized Gap", f"{apq_gap_mean * 100:.2f} pp")
        else:
            c4.metric("Avg APQ vs Normalized Gap", "N/A")
    else:
        c4.metric("Avg APQ vs Normalized Gap", "N/A")

    left, right = st.columns(2, gap="large")
    with left:
        st.markdown("**Missing Field Severity**")
        if present_critical:
            miss = (
                pd.DataFrame(
                    {
                        "field": present_critical,
                        "missing_rate": [(1.0 - float(work[c].notna().mean())) for c in present_critical],
                    }
                )
                .sort_values("missing_rate", ascending=False)
            )
            miss_chart = (
                alt.Chart(miss)
                .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
                .encode(
                    x=alt.X("field:N", sort=miss["field"].tolist(), title="Field"),
                    y=alt.Y("missing_rate:Q", axis=alt.Axis(format=".0%"), title="Missing Rate"),
                    color=alt.Color("missing_rate:Q", scale=alt.Scale(scheme="orangered"), title="Missing"),
                    tooltip=[
                        alt.Tooltip("field:N", title="Field"),
                        alt.Tooltip("missing_rate:Q", title="Missing Rate", format=".3f"),
                    ],
                )
                .properties(height=280)
            )
            st.altair_chart(miss_chart, width='stretch')
        else:
            st.info("No critical fields available for missing-value diagnostics.")

    with right:
        st.markdown("**Source-Type Data Quality**")
        if source_col and present_critical:
            source_rows = []
            for source, g in work.groupby(source_col, dropna=False):
                source_rows.append(
                    {
                        "data_source_type": str(source),
                        "rows": int(len(g)),
                        "critical_completeness": float(g[present_critical].notna().all(axis=1).mean()),
                    }
                )
            source_df = pd.DataFrame(source_rows).sort_values("critical_completeness", ascending=False)
            source_chart = (
                alt.Chart(source_df)
                .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
                .encode(
                    x=alt.X("data_source_type:N", title="Source Type"),
                    y=alt.Y("critical_completeness:Q", axis=alt.Axis(format=".0%"), title="Critical Completeness"),
                    color=alt.Color("critical_completeness:Q", scale=alt.Scale(scheme="greens"), title="Completeness"),
                    tooltip=[
                        alt.Tooltip("data_source_type:N", title="Source"),
                        alt.Tooltip("rows:Q", title="Rows"),
                        alt.Tooltip("critical_completeness:Q", title="Completeness", format=".3f"),
                    ],
                )
                .properties(height=280)
            )
            st.altair_chart(source_chart, width='stretch')
        else:
            st.info("`data_source_type` not available for source-level diagnostics.")

    if source_col and {"oee_normalized", "availability", "performance", "quality"}.issubset(work.columns):
        st.markdown("**OEE Definition Consistency by Source Type**")
        temp = work.copy()
        temp["__calc_oee"] = (
            pd.to_numeric(temp["availability"], errors="coerce")
            * pd.to_numeric(temp["performance"], errors="coerce")
            * pd.to_numeric(temp["quality"], errors="coerce")
        )
        temp["__norm_oee"] = pd.to_numeric(temp["oee_normalized"], errors="coerce")
        temp["__gap"] = (temp["__calc_oee"] - temp["__norm_oee"]).abs()
        gap_df = (
            temp.dropna(subset=["__gap"])
            .groupby(source_col, as_index=False)["__gap"]
            .mean()
            .rename(columns={"__gap": "avg_definition_gap"})
            .sort_values("avg_definition_gap", ascending=False)
        )
        if not gap_df.empty:
            gap_chart = (
                alt.Chart(gap_df)
                .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
                .encode(
                    x=alt.X(f"{source_col}:N", title="Source Type"),
                    y=alt.Y("avg_definition_gap:Q", axis=alt.Axis(format=".0%"), title="Avg |A*P*Q - OEE_Normalized|"),
                    color=alt.Color("avg_definition_gap:Q", scale=alt.Scale(scheme="reds"), title="Gap"),
                    tooltip=[
                        alt.Tooltip(f"{source_col}:N", title="Source"),
                        alt.Tooltip("avg_definition_gap:Q", title="Avg Gap", format=".4f"),
                    ],
                )
                .properties(height=260)
            )
            st.altair_chart(gap_chart, width='stretch')

    st.markdown("**Unit & Runtime Anomaly Flags**")
    anomaly_cols = [c for c in ["planned_time_min", "run_time_min", "downtime_min"] if c in work.columns]
    anomaly_rows: list[dict[str, object]] = []
    if plant_col and anomaly_cols:
        group_cols = [plant_col] + ([source_col] if source_col else [])
        for keys, g in work.groupby(group_cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            record: dict[str, object] = {plant_col: str(keys[0])}
            if source_col:
                record[source_col] = str(keys[1])
            for col in anomaly_cols:
                s = pd.to_numeric(g[col], errors="coerce").dropna()
                if s.empty:
                    continue
                med = float(s.median())
                if 0 < med < 60:
                    anomaly_rows.append(
                        {
                            **record,
                            "field": col,
                            "median_value": med,
                            "issue": "Possible hours-vs-minutes mismatch",
                        }
                    )
                elif med > 24 * 60 * 3:
                    anomaly_rows.append(
                        {
                            **record,
                            "field": col,
                            "median_value": med,
                            "issue": "Unusually high minutes value",
                        }
                    )

    if anomaly_rows:
        anomaly_df = pd.DataFrame(anomaly_rows).sort_values("median_value", ascending=False)
        st.dataframe(anomaly_df, width='stretch')
    else:
        st.success("No obvious unit anomalies detected from median runtime fields.")

    actions: list[str] = []
    if present_critical:
        miss_rates = {c: 1.0 - float(work[c].notna().mean()) for c in present_critical}
        for field in ["downtime_min", "performance", "quality"]:
            if field in miss_rates and miss_rates[field] > 0.15:
                actions.append(
                    f"Prioritize derivation/validation for `{field}` (missing {miss_rates[field] * 100:.1f}%)."
                )
    if reported_gap_mean is not None and reported_gap_mean > 0.05:
        actions.append(
            f"Investigate `oee_reported` definition drift (avg gap {reported_gap_mean * 100:.2f} pp)."
        )
    if apq_gap_mean is not None and apq_gap_mean > 0.05:
        actions.append(
            f"Validate Availability/Performance/Quality formulas (avg gap {apq_gap_mean * 100:.2f} pp)."
        )
    if anomaly_rows:
        actions.append("Standardize time-unit conversion rules for flagged plants/source-types.")

    st.markdown("**Recommended Next Actions**")
    if actions:
        for action in actions:
            st.write(f"- {action}")
    else:
        st.write("- No high-severity normalization issues detected on current filtered scope.")


def _plant_column(df: pd.DataFrame) -> str | None:
    if "plant_name" in df.columns:
        return "plant_name"
    if "plant_id" in df.columns:
        return "plant_id"
    return None


def _render_primary_charts(df: pd.DataFrame) -> None:
    plant_col = _plant_column(df)
    left, right = st.columns(2, gap="large")

    with left:
        st.subheader("📈 OEE Trend")
        if plant_col and {"date", "oee_normalized", plant_col}.issubset(df.columns):
            trend_df = (
                df.dropna(subset=["date", "oee_normalized", plant_col])
                .groupby(["date", plant_col], as_index=False)["oee_normalized"]
                .mean()
                .sort_values("date")
            )
            if trend_df.empty:
                st.info("No data for OEE trend.")
            else:
                top_plants = (
                    trend_df.groupby(plant_col, as_index=False)["oee_normalized"]
                    .mean()
                    .sort_values("oee_normalized", ascending=False)[plant_col]
                    .tolist()
                )
                default_plants = top_plants[: min(6, len(top_plants))]
                selected_plants = st.multiselect(
                    "Plants in trend",
                    options=top_plants,
                    default=default_plants,
                    key="trend_plants_select",
                )
                if selected_plants:
                    trend_df = trend_df[trend_df[plant_col].isin(selected_plants)]

                line_chart = (
                    alt.Chart(trend_df)
                    .mark_line(point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y("oee_normalized:Q", title="OEE Normalized", axis=alt.Axis(format=".0%")),
                        color=alt.Color(f"{plant_col}:N", title="Plant"),
                        tooltip=[
                            alt.Tooltip("date:T", title="Date"),
                            alt.Tooltip(f"{plant_col}:N", title="Plant"),
                            alt.Tooltip("oee_normalized:Q", title="OEE", format=".3f"),
                        ],
                    )
                    .properties(height=340)
                    .interactive()
                )
                st.altair_chart(line_chart, width='stretch')
        else:
            st.info("Missing required columns for OEE trend.")

    with right:
        st.subheader("🏆 OEE by Plant")
        if plant_col and {"oee_normalized", plant_col}.issubset(df.columns):
            agg = (
                df.dropna(subset=["oee_normalized", plant_col])
                .groupby(plant_col, as_index=False)["oee_normalized"]
                .agg(avg="mean", min="min", max="max", count="count")
                .sort_values("avg", ascending=False)
            )
            if agg.empty:
                st.info("No data for plant comparison.")
            else:
                if len(agg) <= 3:
                    top_n = len(agg)
                else:
                    top_n_default = min(12, len(agg))
                    top_n = st.slider(
                        "Top plants",
                        min_value=3,
                        max_value=len(agg),
                        value=top_n_default,
                        key="top_plants_slider",
                    )
                top_agg = agg.head(max(1, top_n)).copy()
                ordered = top_agg[plant_col].tolist()
                bar = (
                    alt.Chart(top_agg)
                    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                    .encode(
                        x=alt.X(f"{plant_col}:N", sort=ordered, title="Plant"),
                        y=alt.Y("avg:Q", title="Average OEE", axis=alt.Axis(format=".0%")),
                        color=alt.Color("avg:Q", title="Avg OEE", scale=alt.Scale(scheme="teals")),
                        tooltip=[
                            alt.Tooltip(f"{plant_col}:N", title="Plant"),
                            alt.Tooltip("avg:Q", title="Avg OEE", format=".3f"),
                            alt.Tooltip("min:Q", title="Min OEE", format=".3f"),
                            alt.Tooltip("max:Q", title="Max OEE", format=".3f"),
                            alt.Tooltip("count:Q", title="Rows"),
                        ],
                    )
                )
                span = (
                    alt.Chart(top_agg)
                    .mark_rule(color="#0f172a", opacity=0.45)
                    .encode(
                        x=alt.X(f"{plant_col}:N", sort=ordered),
                        y=alt.Y("min:Q"),
                        y2="max:Q",
                    )
                )
                st.altair_chart((bar + span).properties(height=340), width='stretch')
        else:
            st.info("Missing required columns for OEE by plant chart.")


def _render_advanced_charts(df: pd.DataFrame) -> None:
    plant_col = _plant_column(df)
    left, right = st.columns(2, gap="large")

    with left:
        st.subheader("⏱️ Runtime vs Downtime")
        if plant_col and {plant_col, "run_time_min", "downtime_min"}.issubset(df.columns):
            runtime_df = (
                df.dropna(subset=[plant_col])
                .groupby(plant_col, as_index=False)[["run_time_min", "downtime_min"]]
                .sum()
            )
            runtime_df = runtime_df.melt(
                id_vars=[plant_col],
                value_vars=["run_time_min", "downtime_min"],
                var_name="metric",
                value_name="minutes",
            )
            runtime_df["metric"] = runtime_df["metric"].map(
                {"run_time_min": "Run Time", "downtime_min": "Downtime"}
            )
            stacked = (
                alt.Chart(runtime_df)
                .mark_bar()
                .encode(
                    x=alt.X(f"{plant_col}:N", title="Plant"),
                    y=alt.Y("minutes:Q", title="Minutes"),
                    color=alt.Color("metric:N", title="Metric"),
                    tooltip=[
                        alt.Tooltip(f"{plant_col}:N", title="Plant"),
                        alt.Tooltip("metric:N", title="Metric"),
                        alt.Tooltip("minutes:Q", title="Minutes", format=".1f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(stacked, width='stretch')
        else:
            st.info("Missing required columns for runtime/downtime analysis.")

    with right:
        st.subheader("🔗 A/P/Q Relationship")
        required = {"availability", "performance", "quality"}
        if plant_col and required.issubset(df.columns):
            apq_df = (
                df.dropna(subset=[plant_col])
                .groupby(plant_col, as_index=False)[["availability", "performance", "quality"]]
                .mean()
            )
            if "oee_normalized" in df.columns:
                oee_df = (
                    df.dropna(subset=[plant_col])
                    .groupby(plant_col, as_index=False)["oee_normalized"]
                    .mean()
                    .rename(columns={"oee_normalized": "avg_oee"})
                )
                apq_df = apq_df.merge(oee_df, on=plant_col, how="left")
            tooltip = [
                alt.Tooltip(f"{plant_col}:N", title="Plant"),
                alt.Tooltip("availability:Q", title="Availability", format=".3f"),
                alt.Tooltip("performance:Q", title="Performance", format=".3f"),
                alt.Tooltip("quality:Q", title="Quality", format=".3f"),
            ]
            encode_kwargs = {
                "x": alt.X("performance:Q", title="Performance", axis=alt.Axis(format=".0%")),
                "y": alt.Y("quality:Q", title="Quality", axis=alt.Axis(format=".0%")),
                "color": alt.Color("availability:Q", title="Availability", scale=alt.Scale(scheme="blues")),
                "tooltip": tooltip,
            }
            if "avg_oee" in apq_df.columns:
                encode_kwargs["size"] = alt.Size("avg_oee:Q", title="Avg OEE")
                tooltip.append(alt.Tooltip("avg_oee:Q", title="Avg OEE", format=".3f"))
                bubble_mark = alt.Chart(apq_df).mark_circle(opacity=0.85, stroke="#0f172a", strokeWidth=0.5)
            else:
                bubble_mark = alt.Chart(apq_df).mark_circle(size=120, opacity=0.85, stroke="#0f172a", strokeWidth=0.5)

            bubble = bubble_mark.encode(**encode_kwargs).properties(height=320).interactive()
            st.altair_chart(bubble, width='stretch')
        else:
            st.info("Missing required columns for A/P/Q relationship chart.")

    st.subheader("🔥 Plant-Date OEE Heatmap")
    if plant_col and {"date", "oee_normalized", plant_col}.issubset(df.columns):
        heat_df = (
            df.dropna(subset=["date", "oee_normalized", plant_col])
            .groupby(["date", plant_col], as_index=False)["oee_normalized"]
            .mean()
            .sort_values("date")
        )
        if heat_df.empty:
            st.info("No data for OEE heatmap.")
        else:
            unique_dates = sorted(heat_df["date"].dropna().unique().tolist())
            if len(unique_dates) > 45:
                keep_dates = set(unique_dates[-45:])
                heat_df = heat_df[heat_df["date"].isin(keep_dates)]

            heatmap = (
                alt.Chart(heat_df)
                .mark_rect()
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y(f"{plant_col}:N", title="Plant"),
                    color=alt.Color("oee_normalized:Q", title="OEE", scale=alt.Scale(scheme="yellowgreenblue")),
                    tooltip=[
                        alt.Tooltip("date:T", title="Date"),
                        alt.Tooltip(f"{plant_col}:N", title="Plant"),
                        alt.Tooltip("oee_normalized:Q", title="Avg OEE", format=".3f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(heatmap, width='stretch')
    else:
        st.info("Missing required columns for OEE heatmap.")


def main() -> None:
    st.set_page_config(page_title="OEE Analytics Dashboard", layout="wide")
    st.title("📊 OEE Unified Analytics Dashboard")
    st.caption("🏭 Real-time manufacturing KPI insights powered by Exasol • 🤖 AI-assisted SQL generation")
    
    # ⚡ AUTO-REFRESH MECHANISM: Time-based trigger with st.rerun()
    # Initialize auto-refresh state EARLY to allow dialog detection
    if "auto_refresh_enabled" not in st.session_state:
        st.session_state["auto_refresh_enabled"] = False  # Disabled by default
    if "show_quick_ask_dialog" not in st.session_state:
        st.session_state["show_quick_ask_dialog"] = False
    if "last_refresh_time" not in st.session_state:
        st.session_state["last_refresh_time"] = time.time()
    
    # Disable auto-refresh when Quick Ask dialog is open
    if st.session_state.get("show_quick_ask_dialog", False):
        st.session_state["auto_refresh_enabled"] = False
    
    # Check if it's time to refresh
    refresh_interval = int(os.getenv("ANALYTICS_REFRESH_INTERVAL_SEC", "2"))
    current_time = time.time()
    time_elapsed = current_time - st.session_state["last_refresh_time"]
    
    # Auto-refresh only if enabled and dialog is not open
    should_refresh = (
        st.session_state.get("auto_refresh_enabled", True) 
        and time_elapsed >= (refresh_interval - 0.5)
    )
    
    if should_refresh:
        st.session_state["last_refresh_time"] = current_time
        st.session_state["data_refresh_nonce"] = st.session_state.get("data_refresh_nonce", 0) + 1
        load_unified_data.clear()
        invalidate_cache()
        st.rerun()

    _load_env_file()
    default_schema = os.getenv("EXASOL_SCHEMA", "HACKATHON")
    default_table = os.getenv("EXASOL_TABLE", "OEE_UNIFIED")

    schema = default_schema
    table = default_table

    sql_provider = os.getenv("TEXT2SQL_PROVIDER", "ollama").strip().lower()

    # Initialize remaining session state variables
    if "data_refresh_nonce" not in st.session_state:
        st.session_state["data_refresh_nonce"] = 0
    if "latest_filtered_df" not in st.session_state:
        st.session_state["latest_filtered_df"] = pd.DataFrame()
    if "latest_available_columns" not in st.session_state:
        st.session_state["latest_available_columns"] = []
    if "latest_refresh_label" not in st.session_state:
        st.session_state["latest_refresh_label"] = "Not refreshed yet"
    if "pending_question" not in st.session_state:
        st.session_state["pending_question"] = None
    
    # Re-enable auto-refresh if dialog was just closed
    if st.session_state.get("dialog_was_open", False):
        st.session_state["auto_refresh_enabled"] = True
        st.session_state["dialog_was_open"] = False

    with st.expander("⚙️ Runtime Controls", expanded=False):
        st.caption(f"🎯 Target: `{schema}.{table}` | 🤖 AI: `{sql_provider}`")
        col1, col2, col3 = st.columns([1.5, 1.2, 1.3])
        with col1:
            row_limit = st.slider("📈 Data Limit", 100, 1000000, 50000, 100)
        with col2:
            st.session_state["auto_refresh_enabled"] = st.checkbox(
                "🔄 Auto-Refresh",
                value=st.session_state["auto_refresh_enabled"],
                help="Automatically refresh data when changes detected"
            )
        with col3:
            if st.button("🔃 Manual Refresh", width='stretch'):
                st.session_state["data_refresh_nonce"] += 1
                load_unified_data.clear()
                invalidate_cache()
                st.rerun()
        
        # Auto-refresh interval configuration (real-time analytics)
        current_refresh_interval = int(os.getenv("ANALYTICS_REFRESH_INTERVAL_SEC", "2"))
        st.caption(
            f"🕐 **Real-Time Refresh:** Every {current_refresh_interval} second(s) • "
            f"📊 Columns: {len(st.session_state.get('latest_available_columns', []))} | "
            f"⏰ Updated: {st.session_state.get('latest_refresh_label', 'Never')}"
        )
        
        # Real-time status indicator
        refresh_interval = int(os.getenv("ANALYTICS_REFRESH_INTERVAL_SEC", "2"))
        if st.session_state.get("auto_refresh_enabled", True):
            refresh_status = "🟢 **LIVE** (Real-time mode)"
            refresh_detail = f"Refreshing every {refresh_interval}s"
        else:
            refresh_status = "⏸️ **PAUSED** (Quick Ask in progress)"
            refresh_detail = "Auto-refresh resumed when dialog closes"
        
        st.info(f"{refresh_status} • {refresh_detail}", icon="🔄")

    initial_refresh_bucket = 0
    initial_refresh_key = (
        f"{st.session_state['data_refresh_nonce']}:0:{schema}:{table}:{row_limit}"
    )
    try:
        initial_raw_df, initial_cols = load_unified_data(
            schema=schema,
            table=table,
            limit=row_limit,
            refresh_key=initial_refresh_key,
        )
        st.session_state["latest_available_columns"] = initial_cols
        st.session_state["latest_refresh_label"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not initial_raw_df.empty:
            st.session_state["latest_filtered_df"] = _prepare_dataframe(initial_raw_df).copy()
    except Exception as exc:
        st.error(f"Initial data snapshot failed: {exc}")

    top_left, top_right = st.columns([2.0, 1.5], gap="medium")
    with top_left:
        st.markdown("### 📊 OEE Trend Snapshot")
        st.caption("⚡ Quick view from latest data load")
        left_df = st.session_state.get("latest_filtered_df", pd.DataFrame())
        if not isinstance(left_df, pd.DataFrame):
            left_df = pd.DataFrame()

        if not left_df.empty and {"date", "oee_normalized"}.issubset(left_df.columns):
            trend_df = left_df.copy()
            trend_df["date"] = pd.to_datetime(trend_df["date"], errors="coerce")
            trend_df["oee_normalized"] = pd.to_numeric(trend_df["oee_normalized"], errors="coerce")
            trend_df = trend_df.dropna(subset=["date", "oee_normalized"])
            trend_df = (
                trend_df.groupby("date", as_index=False)["oee_normalized"]
                .mean()
                .sort_values("date")
            )
            if len(trend_df) > 60:
                trend_df = trend_df.tail(60)

            if not trend_df.empty:
                overview_chart = (
                    alt.Chart(trend_df)
                    .mark_line(point=True, strokeWidth=2.5, color="#0f766e")
                    .encode(
                        x=alt.X("date:T", title="Date"),
                        y=alt.Y("oee_normalized:Q", title="Avg OEE", axis=alt.Axis(format=".0%")),
                        tooltip=[
                            alt.Tooltip("date:T", title="Date"),
                            alt.Tooltip("oee_normalized:Q", title="Avg OEE", format=".3f"),
                        ],
                    )
                    .properties(height=240)
                    .interactive()
                )
                st.altair_chart(overview_chart, width='stretch')
            else:
                st.info("No valid rows available for OEE trend preview.")
        else:
            st.info("Load data to render the OEE trend preview.")
            
    with top_right:
        with st.container(border=True):
            st.markdown("### 🤖 Quick Ask")
            snapshot_cols = st.session_state.get("latest_available_columns", [])
            snapshot_df = st.session_state.get("latest_filtered_df", pd.DataFrame())
            if not isinstance(snapshot_df, pd.DataFrame):
                snapshot_df = pd.DataFrame()
            
            # Improved compact UI for Quick Ask
            _render_quick_ask_panel(
                schema=schema,
                table=table,
                available_columns=snapshot_cols or list(COLUMN_CANDIDATES.keys()),
                sql_provider=sql_provider,
                fallback_df=snapshot_df if not snapshot_df.empty else None,
            )

    st.divider()

    def render_live_analytics_body() -> None:
        try:
            refresh_key = (
                f"{st.session_state['data_refresh_nonce']}:0:{schema}:{table}:{row_limit}"
            )

            raw_df, available_columns = load_unified_data(
                schema=schema,
                table=table,
                limit=row_limit,
                refresh_key=refresh_key,
            )

            st.session_state["latest_available_columns"] = available_columns
            st.session_state["latest_refresh_label"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.caption(
                f"Columns loaded: {len(available_columns)} | "
                f"Last analytics refresh: {st.session_state['latest_refresh_label']}"
            )

            if raw_df.empty:
                st.warning("No rows found in target table.")
                st.session_state["latest_filtered_df"] = pd.DataFrame()
                return

            df = _prepare_dataframe(raw_df)
            filtered = df.copy()
            st.session_state["latest_filtered_df"] = filtered.copy()

            # SECTION 1: Comprehensive KPI Dashboard at Top
            _render_comprehensive_kpi_dashboard(filtered)
            st.divider()
            
            # SECTION 2: Main Analytics Charts (GRAPHS AT TOP)
            _render_primary_charts(filtered)
            st.divider()
            _render_advanced_charts(filtered)
            st.divider()
            
            # SECTION 3: Gap Analysis & Data Quality
            _render_oee_gap_analysis(filtered)
            st.divider()
            
            _render_mes_data_quality_dashboard(filtered)
            st.divider()
            
            _render_basic_line_chart(filtered)
            st.divider()
            
            # SECTION 4: Data Table
            st.subheader("📋 Filtered Data")
            st.dataframe(
                filtered.sort_values(by="date") if "date" in filtered.columns else filtered,
                width='stretch',
            )
            st.divider()
            
            # SECTION 5: Normalization Problem Solver (AT BOTTOM)
            _render_normalization_problem_solver(filtered)
            
        except Exception as exc:
            st.error(f"Live analytics render failed: {exc}")

    render_live_analytics_body()

    with st.expander("💬 Ask Data (Full View)"):
        snapshot_cols = st.session_state.get("latest_available_columns", [])
        snapshot_df = st.session_state.get("latest_filtered_df", pd.DataFrame())
        if not isinstance(snapshot_df, pd.DataFrame):
            snapshot_df = pd.DataFrame()
        _render_ask_data(
            schema=schema,
            table=table,
            available_columns=snapshot_cols or list(COLUMN_CANDIDATES.keys()),
            sql_provider=sql_provider,
            model_override=None,
            compact=False,
            key_prefix="ask_full",
            fallback_df=snapshot_df if not snapshot_df.empty else None,
        )


if __name__ == "__main__":
    main()
