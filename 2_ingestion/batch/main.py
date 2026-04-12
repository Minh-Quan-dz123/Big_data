"""
Batch Data Ingestion Pipeline.
Dataset → Đọc + Clean → Ghi Avro → Upload HDFS.
"""
from __future__ import annotations

import os
import sys
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv

from configs import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# File → schema (không gồm events.csv/event.json)
FILE_TABLE_MAP = {
    "users.csv":       "users",
    "products.csv":    "products",
    "orders.csv":      "orders",
    "order_items.csv": "order_items",
    "reviews.csv":     "reviews",
}
EXCLUDED = {"events.csv", "event.json", ".DS_Store", "Thumbs.db"}


# ─── TODO 1: Clean dữ liệu ──────────────────────────────────────────────────
def clean_table(table: pa.Table) -> pa.Table:
    """
    Strip whitespace, lowercase text, null → "".
    """
    if table.num_rows == 0:
        return table

    records = table.to_pydict()
    cleaned = {}

    for col, values in records.items():
        new_vals = []
        for v in values:
            if v is None or str(v).strip() == "":
                new_vals.append("")
            elif isinstance(v, str):
                new_vals.append(v.strip().lower())
            else:
                new_vals.append(v)
        cleaned[col] = new_vals

    return pa.table(cleaned)


# ─── TODO 2: Ghi Avro + Upload HDFS ────────────────────────────────────────
def write_to_hdfs(local_avro: Path, hdfs_dir: str) -> bool:
    """Tạo thư mục HDFS và upload file Avro."""
    if not local_avro.exists():
        log.error(f"File not found: {local_avro}")
        return False

    # Tạo thư mục HDFS
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
        capture_output=True, check=False,
    )

    hdfs_path = f"{hdfs_dir}/{local_avro.name}"
    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", str(local_avro), hdfs_path],
        capture_output=True, text=True, check=False,
    )
    if result.returncode == 0:
        log.info(f"  [HDFS] {hdfs_path}")
        return True
    log.error(f"HDFS upload failed: {result.stderr}")
    return False


# ─── BATCH PIPELINE ──────────────────────────────────────────────────────────
def run_batch_ingestion():
    print("[BATCH START]")

    cfg = config.Config.from_yaml(Path(__file__).parent / "config.yaml") \
        if (Path(__file__).parent / "config.yaml").exists() \
        else config.Config()

    raw_dir = cfg.RAW_DATA_DIR
    avro_dir = cfg.AVRO_TEMP_DIR
    avro_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Nguồn : {raw_dir}")
    log.info(f"HDFS   : {cfg.HDFS_OUTPUT_BASE}")

    if not raw_dir.exists():
        log.error(f"Thư mục nguồn không tồn tại: {raw_dir}")
        sys.exit(1)

    files = [f for f in raw_dir.glob("*.csv") if f.name not in EXCLUDED and f.name in FILE_TABLE_MAP]
    log.info(f"Tìm thấy {len(files)} file: {[f.name for f in files]}")

    success, failed = 0, 0

    for file_path in files:
        schema = FILE_TABLE_MAP[file_path.name]
        log.info(f"\n▶ {file_path.name}")

        # Đọc CSV bằng PyArrow
        table = pa_csv.read_csv(file_path)
        rows_before = table.num_rows
        log.info(f"  [READ ] {rows_before:,} dòng")

        # Clean
        table_clean = clean_table(table)
        log.info(f"  [CLEAN] {table_clean.num_rows:,} dòng sau clean")

        if table_clean.num_rows == 0:
            failed += 1
            continue

        # Ghi Avro local (dùng avro library)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        avro_file = avro_dir / f"{schema}_{timestamp}.avro"

        import avro
        from avro.datafile import DataFileWriter
        from avro.io import DatumWriter

        fields = [{"name": c, "type": "string"} for c in table_clean.column_names]
        avsc = avro.schema.Parse(json.dumps({
            "type": "record", "name": schema,
            "namespace": "ecommerce.raw",
            "fields": fields,
        }))

        records = [dict(zip(table_clean.column_names, row))
                   for row in zip(*table_clean.to_pydict().values())]

        with open(avro_file, "wb") as f:
            writer = DataFileWriter(f, DatumWriter(), avsc)
            for rec in records:
                writer.append({k: (str(v) if v is not None else "") for k, v in rec.items()})
            writer.close()

        log.info(f"  [AVRO ] {avro_file.name} ({avro_file.stat().st_size:,} bytes)")

        # Upload HDFS
        hdfs_dir = f"{cfg.HDFS_OUTPUT_BASE}/{schema}"
        if write_to_hdfs(avro_file, hdfs_dir):
            success += 1
            log.info(f"  [DONE ] {file_path.name} → {hdfs_dir}")
        else:
            failed += 1

    log.info(f"\n{'='*60}")
    log.info(f"Kết quả: {success} thành công, {failed} thất bại")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    run_batch_ingestion()
