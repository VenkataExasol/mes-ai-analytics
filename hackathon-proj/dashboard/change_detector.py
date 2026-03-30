"""
Change Detection Module for Near Real-Time Analytics

Monitors Exasol tables for data modifications and triggers refresh logic.
Uses timestamp-based change detection to minimize query overhead.
"""

import os
import ssl
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pyexasol

logger = logging.getLogger(__name__)


def _load_env_file() -> None:
    """Load environment variables from .env file."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _connect() -> pyexasol.ExaConnection:
    """Create Exasol database connection."""
    _load_env_file()
    user = os.getenv("EXASOL_USER", "sys")
    password = os.getenv("EXASOL_PASSWORD", "exasol")
    dsn = os.getenv("EXASOL_DSN", "127.0.0.1:8563")
    
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


def get_table_row_count(schema: str, table: str) -> int:
    """Get current row count of table."""
    conn = _connect()
    try:
        stmt = conn.execute(f'SELECT COUNT(*) as cnt FROM "{schema}"."{table}"')
        rows = stmt.fetchall()
        return int(rows[0][0]) if rows else 0
    finally:
        conn.close()


def get_table_max_timestamp(schema: str, table: str, timestamp_col: str = "ts_modified") -> Optional[datetime]:
    """
    Get the maximum timestamp from the table (for tracking last modification).
    Tries common timestamp column names if provided column doesn't exist.
    """
    conn = _connect()
    try:
        # Try the provided column name first
        try:
            stmt = conn.execute(
                f'SELECT MAX("{timestamp_col}") as max_ts FROM "{schema}"."{table}"'
            )
            rows = stmt.fetchall()
            if rows and rows[0][0]:
                max_ts = rows[0][0]
                if isinstance(max_ts, datetime):
                    return max_ts
                return datetime.fromisoformat(str(max_ts))
        except Exception:
            # Fall back to using MAX(date) or insertion order
            pass
        
        # Fallback: use MAX(date) if it exists
        try:
            stmt = conn.execute(f'SELECT MAX("date") as max_date FROM "{schema}"."{table}"')
            rows = stmt.fetchall()
            if rows and rows[0][0]:
                max_date = rows[0][0]
                if isinstance(max_date, datetime):
                    return max_date
                return datetime.fromisoformat(str(max_date))
        except Exception:
            pass
        
        return None
    finally:
        conn.close()


def check_data_changed(
    schema: str,
    table: str,
    last_known_row_count: int,
    last_known_timestamp: Optional[datetime] = None,
) -> Tuple[bool, dict]:
    """
    Check if table data has changed since last check.
    
    Returns:
        (changed: bool, metadata: dict with details)
    """
    metadata = {
        "current_row_count": 0,
        "current_max_timestamp": None,
        "row_count_changed": False,
        "timestamp_changed": False,
        "change_detected": False,
    }
    
    try:
        current_count = get_table_row_count(schema, table)
        current_max_ts = get_table_max_timestamp(schema, table)
        
        metadata["current_row_count"] = current_count
        metadata["current_max_timestamp"] = current_max_ts
        
        # Check if row count changed
        if current_count != last_known_row_count:
            metadata["row_count_changed"] = True
            metadata["row_count_diff"] = current_count - last_known_row_count
            metadata["change_detected"] = True
        
        # Check if timestamp changed (indicates data modification)
        if last_known_timestamp and current_max_ts and current_max_ts > last_known_timestamp:
            metadata["timestamp_changed"] = True
            metadata["change_detected"] = True
        elif current_max_ts and not last_known_timestamp:
            # First check
            metadata["change_detected"] = False
        
        return metadata["change_detected"], metadata
    except Exception as e:
        logger.error(f"Error checking data changes: {e}")
        return False, metadata


def get_recent_changes(
    schema: str,
    table: str,
    lookback_hours: int = 1,
    timestamp_col: str = "date",
) -> dict:
    """
    Get summary of recent changes in the table.
    Returns statistics about recently modified data.
    """
    conn = _connect()
    try:
        cutoff_time = datetime.now() - timedelta(hours=lookback_hours)
        
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT "plant_id") as unique_plants,
            COUNT(DISTINCT "date") as unique_dates,
            MIN("{timestamp_col}") as oldest_record,
            MAX("{timestamp_col}") as newest_record
        FROM "{schema}"."{table}"
        WHERE "{timestamp_col}" >= '{cutoff_time.isoformat()}'
        """
        
        stmt = conn.execute(query)
        rows = stmt.fetchall()
        
        if rows and rows[0]:
            return {
                "total_records": int(rows[0][0]),
                "unique_plants": int(rows[0][1]) if rows[0][1] else 0,
                "unique_dates": int(rows[0][2]) if rows[0][2] else 0,
                "oldest_record": rows[0][3],
                "newest_record": rows[0][4],
                "lookback_hours": lookback_hours,
            }
        return {}
    except Exception as e:
        logger.error(f"Error getting recent changes: {e}")
        return {}
    finally:
        conn.close()


if __name__ == "__main__":
    # Test the change detector
    logging.basicConfig(level=logging.INFO)
    
    schema = os.getenv("EXASOL_SCHEMA", "HACKATHON")
    table = os.getenv("EXASOL_TABLE", "OEE_UNIFIED")
    
    print(f"Checking table: {schema}.{table}")
    count = get_table_row_count(schema, table)
    print(f"Current row count: {count}")
    
    max_ts = get_table_max_timestamp(schema, table)
    print(f"Maximum timestamp: {max_ts}")
    
    recent = get_recent_changes(schema, table, lookback_hours=24)
    print(f"Recent changes (24h): {recent}")
