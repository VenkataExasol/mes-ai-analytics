"""
Async Query Worker Module for Near Real-Time Analytics

Executes Exasol queries on background threads to prevent UI blocking.
Manages query result caching and thread-safe communication with Streamlit.
"""

import os
import ssl
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Callable, Any, Dict
from queue import Queue, Empty

import pyexasol
import pandas as pd

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


class QueryCache:
    """Simple in-memory query cache with TTL."""
    
    def __init__(self, ttl_seconds: int = 300):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.ttl_seconds = ttl_seconds
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Optional[pd.DataFrame]:
        """Get cached result if not expired."""
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]
                if datetime.now() < entry["expires_at"]:
                    return entry["result"]
                else:
                    del self.cache[key]
        return None
    
    def set(self, key: str, value: pd.DataFrame) -> None:
        """Cache result with TTL."""
        with self.lock:
            self.cache[key] = {
                "result": value,
                "expires_at": datetime.now() + timedelta(seconds=self.ttl_seconds),
                "cached_at": datetime.now(),
            }
    
    def invalidate(self, pattern: Optional[str] = None) -> None:
        """Invalidate cache entries matching pattern or all if pattern is None."""
        with self.lock:
            if pattern is None:
                self.cache.clear()
            else:
                keys_to_remove = [k for k in self.cache.keys() if pattern in k]
                for key in keys_to_remove:
                    del self.cache[key]
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        with self.lock:
            return {
                "size": len(self.cache),
                "ttl_seconds": self.ttl_seconds,
            }


# Global cache instance
_query_cache = QueryCache(ttl_seconds=int(os.getenv("ANALYTICS_CACHE_TTL_SEC", "300")))


def execute_query(sql: str, cache_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Execute SQL query and return results as DataFrame.
    Uses cache if cache_key is provided.
    """
    # Check cache first
    if cache_key:
        cached = _query_cache.get(cache_key)
        if cached is not None:
            logger.debug(f"Cache hit for key: {cache_key}")
            return cached
    
    conn = _connect()
    try:
        stmt = conn.execute(sql)
        rows = stmt.fetchall()
        
        # Get column names
        meta = stmt.columns()
        if isinstance(meta, dict):
            columns = list(meta.keys())
        elif isinstance(meta[0], dict):
            columns = [col.get("name", "") for col in meta]
        else:
            columns = [str(col) for col in meta]
        
        df = pd.DataFrame(rows, columns=columns)
        
        # Cache result if key provided
        if cache_key:
            _query_cache.set(cache_key, df)
            logger.debug(f"Cached result for key: {cache_key}")
        
        return df
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        return None
    finally:
        conn.close()


class AsyncQueryWorker:
    """Background worker for executing queries asynchronously."""
    
    def __init__(self, thread_name: str = "AsyncQueryWorker"):
        self.thread_name = thread_name
        self.task_queue: Queue = Queue()
        self.result_queue: Queue = Queue()
        self.running = False
        self.thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        """Start the background worker thread."""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name=self.thread_name,
        )
        self.thread.start()
        logger.info(f"Started {self.thread_name}")
    
    def stop(self) -> None:
        """Stop the background worker thread."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info(f"Stopped {self.thread_name}")
    
    def _worker_loop(self) -> None:
        """Main worker loop - processes tasks from queue."""
        while self.running:
            try:
                # Get task with timeout to check running flag periodically
                task = self.task_queue.get(timeout=1)
                if task is None:  # Poison pill
                    break
                
                task_id = task.get("id")
                sql = task.get("sql")
                cache_key = task.get("cache_key")
                callback = task.get("callback")
                
                try:
                    logger.debug(f"Executing task {task_id}")
                    result = execute_query(sql, cache_key)
                    
                    self.result_queue.put({
                        "task_id": task_id,
                        "success": True,
                        "result": result,
                        "error": None,
                        "timestamp": datetime.now().isoformat(),
                    })
                    
                    # Call callback if provided
                    if callback:
                        try:
                            callback(result)
                        except Exception as e:
                            logger.error(f"Callback failed for task {task_id}: {e}")
                    
                except Exception as e:
                    logger.error(f"Task {task_id} failed: {e}")
                    self.result_queue.put({
                        "task_id": task_id,
                        "success": False,
                        "result": None,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    })
                
                self.task_queue.task_done()
            except Empty:
                continue
    
    def submit_query(
        self,
        sql: str,
        task_id: str,
        cache_key: Optional[str] = None,
        callback: Optional[Callable] = None,
    ) -> None:
        """Submit a query task to the worker."""
        if not self.running:
            raise RuntimeError(f"{self.thread_name} is not running")
        
        task = {
            "id": task_id,
            "sql": sql,
            "cache_key": cache_key,
            "callback": callback,
            "submitted_at": datetime.now().isoformat(),
        }
        self.task_queue.put(task)
        logger.debug(f"Submitted task {task_id}")
    
    def get_result(self, task_id: str, timeout: float = 1.0) -> Optional[dict]:
        """Get result of a completed task."""
        try:
            while True:
                result = self.result_queue.get(timeout=timeout)
                if result["task_id"] == task_id:
                    return result
        except Empty:
            return None
    
    def has_pending_tasks(self) -> bool:
        """Check if there are pending tasks."""
        return not self.task_queue.empty()


# Global worker instance
_query_worker: Optional[AsyncQueryWorker] = None


def get_worker() -> AsyncQueryWorker:
    """Get or create the global query worker."""
    global _query_worker
    if _query_worker is None:
        _query_worker = AsyncQueryWorker()
        _query_worker.start()
    return _query_worker


def invalidate_cache(pattern: Optional[str] = None) -> None:
    """Invalidate cache entries."""
    _query_cache.invalidate(pattern)


def get_cache_stats() -> dict:
    """Get cache statistics."""
    return _query_cache.get_stats()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    # Test sync query execution
    sql = 'SELECT COUNT(*) as cnt FROM "HACKATHON"."OEE_UNIFIED"'
    result = execute_query(sql)
    print(f"Result: {result}")
