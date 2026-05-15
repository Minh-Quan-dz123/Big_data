"""
Batch Config.
"""
from __future__ import annotations

import os
from pathlib import Path
from dataclasses import dataclass, field

import yaml


# Thu muc goc project = 2 cap tren batch/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DEFAULT_RAW_DATA_DIR = _PROJECT_ROOT / "1_dataset" / "raw_data" / "ecommerce_dataset"
DEFAULT_AVRO_TEMP = Path(__file__).parent / "avro_temp"

DEFAULT_HDFS_NAMENODE    = os.getenv("HDFS_NAMENODE", "http://localhost:9870")
DEFAULT_HDFS_USER        = os.getenv("HDFS_USER", "hdfs")
DEFAULT_HDFS_OUTPUT_BASE = "/data_lake/raw"


@dataclass
class Config:
    namenode:    str  = DEFAULT_HDFS_NAMENODE
    user:        str  = DEFAULT_HDFS_USER
    output_base: str  = DEFAULT_HDFS_OUTPUT_BASE
    raw_data_dir: Path = field(default_factory=lambda: DEFAULT_RAW_DATA_DIR)
    avro_temp:   Path = field(default_factory=lambda: DEFAULT_AVRO_TEMP)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Config":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config not found: {path}")
        with open(p, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        hdfs = raw.get("hdfs", {})
        paths = raw.get("paths", {})

        _batch_dir = Path(__file__).parent
        _root = _batch_dir.parent.parent

        raw_dir_raw = paths.get("raw_data_dir", str(DEFAULT_RAW_DATA_DIR))
        avro_raw = paths.get("avro_temp", str(DEFAULT_AVRO_TEMP))

        # Neu la duong dan tuong doi -> resolve tu project root
        raw_data_path = _root / raw_dir_raw if not Path(raw_dir_raw).is_absolute() else Path(raw_dir_raw)
        avro_path = _batch_dir / avro_raw if not Path(avro_raw).is_absolute() else Path(avro_raw)

        return cls(
            namenode    = hdfs.get("namenode",    DEFAULT_HDFS_NAMENODE),
            user        = hdfs.get("user",        DEFAULT_HDFS_USER),
            output_base = hdfs.get("output_base",  DEFAULT_HDFS_OUTPUT_BASE),
            raw_data_dir= raw_data_path.resolve(),
            avro_temp   = avro_path.resolve(),
        )

    @property
    def RAW_DATA_DIR(self) -> Path:
        return self.raw_data_dir

    @property
    def HDFS_NAMENODE(self) -> str:
        return self.namenode

    @property
    def HDFS_OUTPUT_BASE(self) -> str:
        return self.output_base

    @property
    def AVRO_TEMP_DIR(self) -> Path:
        return self.avro_temp
