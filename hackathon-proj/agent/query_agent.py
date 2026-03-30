import os
import re
import ssl
import time
from pathlib import Path

import pyexasol


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


# 🔌 Connect to Exasol
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
                )
            except Exception as exc:
                last_error = exc

        if attempt < 12:
            print(
                f"⚠️ Exasol connection attempt {attempt}/12 failed, retrying in 2s..."
            )
            time.sleep(2)

    raise RuntimeError(f"Failed to connect to Exasol after retries: {last_error}")

# 📊 Fetch and print results
def run():

    conn = connect()
    target_table = _target_fq_table()

    print(f"\n📊 Total Rows in {target_table}:")
    result = conn.execute(f"SELECT COUNT(*) FROM {target_table}")
    for row in result:
        print(f"Total Rows: {row[0]}")

    print("\n📊 Avg OEE per Plant:")
    result = conn.execute("""
        SELECT "plant_id",
               ROUND(AVG("oee_normalized"), 3)
        FROM {target}
        GROUP BY "plant_id"
        ORDER BY 2 DESC
    """.format(target=target_table))

    print("\nPlant | Avg OEE")
    print("-------------------")

    for row in result:
        print(f"{row[0]} | {row[1]}")

    print("\n✅ Query Completed")


if __name__ == "__main__":
    run()
