# etl/utils/metrics.py
import csv
import os
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

DEFAULT_METRICS_CSV = os.environ.get(
    "METRICS_CSV",
    "/opt/airflow/project/logs/etl_metrics.csv"
)

def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def log_metric(
    task_name: str,
    status: str,
    duration_s: float,
    row_count: Optional[int] = None,
    dag_run_id: Optional[str] = None,
    error: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    metrics_csv: Optional[str] = None,
) -> None:
    """
    Registra una fila de métricas en un CSV.
    """
    csv_path = Path(metrics_csv or DEFAULT_METRICS_CSV)
    _ensure_parent(csv_path)

    now = datetime.now(timezone.utc).isoformat()
    row = {
        "timestamp_utc": now,
        "task_name": task_name,
        "status": status,
        "duration_s": f"{duration_s:.4f}",
        "row_count": row_count if row_count is not None else "",
        "dag_run_id": dag_run_id or "",
        "error": (error or "")[:1024],  # evita explosiones por errores enormes
    }

    if extra:
        for k, v in extra.items():
            row[f"extra_{k}"] = v

    file_exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
