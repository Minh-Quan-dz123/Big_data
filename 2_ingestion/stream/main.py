"""
Stream Data Cleaning Pipeline.
Doc tu Kafka topic ABC1 -> Clean -> Gui sang topic ABC2.

Su dung:
    python main.py          # stream mode (loop vo han)
    python main.py batch    # batch mode (doc 1 luot roi thoat)
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Gia tri mac dinh
DEFAULT_KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_INPUT_TOPIC    = "ABC1"
DEFAULT_OUTPUT_TOPIC   = "ABC2"
DEFAULT_CONSUMER_GROUP = "stream-cleaning-group"


def load_config() -> dict:
    """Doc config.yaml neu co."""
    cfg_path = Path(__file__).parent / "config.yaml"
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        kafka = raw.get("kafka", {})
        return {
            "bootstrap_servers": kafka.get("bootstrap_servers", DEFAULT_KAFKA_SERVERS),
            "input_topic":       kafka.get("input_topic",       DEFAULT_INPUT_TOPIC),
            "output_topic":      kafka.get("output_topic",      DEFAULT_OUTPUT_TOPIC),
            "consumer_group":    kafka.get("consumer_group",    DEFAULT_CONSUMER_GROUP),
        }
    return {
        "bootstrap_servers": DEFAULT_KAFKA_SERVERS,
        "input_topic":       DEFAULT_INPUT_TOPIC,
        "output_topic":      DEFAULT_OUTPUT_TOPIC,
        "consumer_group":    DEFAULT_CONSUMER_GROUP,
    }


# Tao Kafka client
def _make_kafka_client(library: str, cfg: dict, mode: str):
    if library == "confluent":
        if mode == "consumer":
            from confluent_kafka import Consumer
            return Consumer({
                "bootstrap.servers":  cfg["bootstrap_servers"],
                "group.id":          cfg["consumer_group"],
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "true",
            })
        else:
            from confluent_kafka import Producer
            return Producer({
                "bootstrap.servers": cfg["bootstrap_servers"],
                "acks":             "all",
                "retries":          "3",
            })

    # kafka-python fallback
    if mode == "consumer":
        from kafka import KafkaConsumer
        return KafkaConsumer(
            cfg["input_topic"],
            bootstrap_servers = cfg["bootstrap_servers"].split(","),
            group_id          = cfg["consumer_group"],
            auto_offset_reset = "earliest",
            value_deserializer = lambda m: m,
        )
    else:
        from kafka import KafkaProducer
        return KafkaProducer(
            bootstrap_servers = cfg["bootstrap_servers"].split(","),
            acks             = 1,
            retries          = 3,
        )


def create_consumer(cfg: dict):
    """Tao consumer doc tu topic input."""
    errors = {}
    for lib in ("confluent", "kafka-python"):
        try:
            client = _make_kafka_client(lib, cfg, "consumer")
            log.info(f"Kafka Consumer: dung '{lib}'")
            return client
        except ImportError as e:
            errors[lib] = str(e)
    raise ImportError(f"Khong tim thay thu vien Kafka: {errors}")


def clean_record(record: dict) -> dict:
    """Strip, lowercase, null -> ''."""
    cleaned = {}
    for key, value in record.items():
        clean_key = key.strip().lower()
        if value is None:
            cleaned[clean_key] = ""
        elif isinstance(value, str):
            cleaned[clean_key] = value.strip().lower()
        else:
            cleaned[clean_key] = value
    return cleaned


def create_producer(cfg: dict):
    """Tao producer gui sang topic output."""
    errors = {}
    for lib in ("confluent", "kafka-python"):
        try:
            client = _make_kafka_client(lib, cfg, "producer")
            log.info(f"Kafka Producer: dung '{lib}'")
            return client
        except ImportError as e:
            errors[lib] = str(e)
    raise ImportError(f"Khong tim thay thu vien Kafka: {errors}")


def run_stream_cleaning():
    print("[STREAM CLEAN START]")

    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "stream"
    cfg = load_config()

    log.info(f"Input  topic : {cfg['input_topic']}")
    log.info(f"Output topic : {cfg['output_topic']}")
    log.info(f"Mode         : {mode}")

    consumer = create_consumer(cfg)
    producer = create_producer(cfg)

    total_in, total_out = 0, 0

    def shutdown(signum, frame):
        log.info("Dong pipeline...")
        consumer.close()
        producer.flush()
        producer.close()
        log.info(f"Ket thuc: vao={total_in} ra={total_out}")
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer.subscribe([cfg["input_topic"]])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if mode == "batch":
                    break
                continue

            raw = msg.value()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")

            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue

            total_in += 1
            cleaned = clean_record(record)
            payload = json.dumps(cleaned, ensure_ascii=False).encode("utf-8")

            # confluent-kafka
            if hasattr(producer, "produce"):
                producer.produce(cfg["output_topic"], value=payload)
            # kafka-python
            else:
                producer.send(cfg["output_topic"], value=payload)

            total_out += 1

            if mode == "batch":
                log.info(f"[BATCH] Done. vao={total_in} ra={total_out}")
                break

            if total_in % 100 == 0:
                producer.flush()
                log.info(f"Progress: vao={total_in} ra={total_out}")

        producer.flush()
        log.info(f"Hoan thanh: vao={total_in} ra={total_out}")

    except KeyboardInterrupt:
        log.warning("Bi ngat boi nguoi dung.")
    finally:
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_stream_cleaning()
