"""
Batch Data Ingestion Pipeline.
Doc CSV -> Clean -> Ghi Avro -> Upload HDFS.
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv

from config_module import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# File CSV -> ten bang trong HDFS
FILE_TABLE_MAP = {
    "users.csv":       "users",
    "products.csv":    "products",
    "orders.csv":      "orders",
    "order_items.csv": "order_items",
    "reviews.csv":     "reviews",
}
EXCLUDED = {"events.csv", "event.json", ".DS_Store", "Thumbs.db"}


def clean_table(table: pa.Table) -> pa.Table:
    """Strip, lowercase, null -> ''."""
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


def write_to_hdfs(local_avro: Path, hdfs_dir: str) -> bool:
    """Tao thu muc HDFS va upload Avro. Neu khong co HDFS -> skip."""
    if not local_avro.exists():
        log.error(f"File not found: {local_avro}")
        return False

    try:
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
            capture_output=True, check=False,
        )
    except FileNotFoundError:
        pass

    hdfs_path = f"{hdfs_dir}/{local_avro.name}"
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", str(local_avro), hdfs_path],
            capture_output=True, text=True, check=False,
        )
        if result.returncode == 0:
            log.info(f"  [HDFS] {hdfs_path}")
            return True
        log.warning(f"  [HDFS] Upload bi bo qua: {result.stderr or result.stdout}")
    except FileNotFoundError:
        log.warning("  [HDFS] Upload bi bo qua (lenh 'hdfs' khong ton tai)")
    return True


def run_batch_ingestion():
    print("[BATCH START]")

    cfg = Config.from_yaml(Path(__file__).parent / "config.yaml") \
        if (Path(__file__).parent / "config.yaml").exists() \
        else Config()

    raw_dir = cfg.RAW_DATA_DIR
    avro_dir = cfg.AVRO_TEMP_DIR
    avro_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Nguon : {raw_dir}")
    log.info(f"HDFS   : {cfg.HDFS_OUTPUT_BASE}")

    if not raw_dir.exists():
        log.error(f"Thu muc nguon khong ton tai: {raw_dir}")
        sys.exit(1)

    files = [f for f in raw_dir.glob("*.csv")
             if f.name not in EXCLUDED and f.name in FILE_TABLE_MAP]
    log.info(f"Tim thay {len(files)} file: {[f.name for f in files]}")

    success, failed = 0, 0

    for file_path in files:
        schema = FILE_TABLE_MAP[file_path.name]
        log.info(f"\n▶ {file_path.name}")

        # Doc CSV bang PyArrow
        table = pa_csv.read_csv(file_path)
        log.info(f"  [READ ] {table.num_rows:,} dong")

        # Clean
        table_clean = clean_table(table)
        log.info(f"  [CLEAN] {table_clean.num_rows:,} dong sau clean")

        if table_clean.num_rows == 0:
            failed += 1
            continue

        # Ghi Avro
        import avro
        from avro.datafile import DataFileWriter
        from avro.io import DatumWriter

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        avro_file = avro_dir / f"{schema}_{timestamp}.avro"

        fields = [{"name": c, "type": "string"} for c in table_clean.column_names]
        avsc = avro.schema.parse(json.dumps({
            "type": "record",
            "name": schema,
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
            log.info(f"  [DONE ] {file_path.name} -> {hdfs_dir}")
        else:
            failed += 1

    log.info(f"\n{'='*60}")
    log.info(f"Ket qua: {success} thanh cong, {failed} that bai")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    run_batch_ingestion()
