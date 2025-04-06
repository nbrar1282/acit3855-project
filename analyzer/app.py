"""Analyzer Service — Reads Kafka events and exposes APIs for event inspection and stats."""

import os
import json
import logging
import logging.config
import time
from typing import Tuple, Any

import connexion
import yaml
from flask import jsonify
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

from kafka_wrapper.kafka_client import KafkaWrapper  # <-- New import

# Use UTC timestamps in logs
logging.Formatter.converter = time.gmtime

# Constants
KAFKA_TIMEOUT_MS = 1000  # Kafka consumer timeout

# Load logging configuration
with open("./config/log_conf.yml", "r", encoding="utf-8") as config_file:
    LOG_CONFIG = yaml.safe_load(config_file.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load application configuration
with open("./config/app_conf.yml", "r", encoding="utf-8") as config_file:
    app_config = yaml.safe_load(config_file.read())

# Kafka connection
KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']

# Use KafkaWrapper
kafka_wrapper = KafkaWrapper(hostname=KAFKA_HOST, topic=TOPIC_NAME, consume_from_start=True)


def get_event_by_index(event_type: str, index: int) -> Tuple[dict, int]:
    """Retrieve a specific event by index for a given event type."""
    counter = 0

    for msg in kafka_wrapper.messages():
        if msg is None:
            break
        try:
            data = json.loads(msg.value.decode("utf-8"))
        except (json.JSONDecodeError, AttributeError) as error:
            logger.warning("Skipping malformed message: %s", str(error))
            continue

        if data.get("type") == event_type:
            if counter == index:
                logger.info("Returning %s event at index %d", event_type, index)
                return data["payload"], 200
            counter += 1

    logger.warning("No %s event found at index %d", event_type, index)
    return {"message": f"No {event_type} event found at index {index}"}, 404


def get_air_quality_event(index: int) -> Tuple[dict, int]:
    """Handle request to get an air quality event by index."""
    return get_event_by_index("air_quality", index)


def get_traffic_flow_event(index: int) -> Tuple[dict, int]:
    """Handle request to get a traffic flow event by index."""
    return get_event_by_index("traffic_flow", index)


def get_event_stats() -> Tuple[Any, int]:
    """Return count of air quality and traffic flow events currently in Kafka."""
    air_quality_count = 0
    traffic_flow_count = 0

    for msg in kafka_wrapper.messages():
        if msg is None:
            break
        try:
            data = json.loads(msg.value.decode("utf-8"))
        except (json.JSONDecodeError, AttributeError) as error:
            logger.warning("Skipping malformed message in stats: %s", str(error))
            continue

        if data.get("type") == "air_quality":
            air_quality_count += 1
        elif data.get("type") == "traffic_flow":
            traffic_flow_count += 1

    stats = {
        "num_air_quality_events": air_quality_count,
        "num_traffic_flow_events": traffic_flow_count
    }

    logger.info("Returning event stats: %s", stats)
    return jsonify(stats), 200


def get_all_air_ids():
    """Get all air quality id and trace_id from Kafka."""
    results = []
    for msg in kafka_wrapper.messages():
        if msg is None:
            break
        try:
            data = json.loads(msg.value.decode("utf-8"))
            if data.get("type") == "air_quality":
                payload = data["payload"]
                results.append({
                    "event_id": payload.get("sensor_id"),
                    "trace_id": payload.get("trace_id")
                })
        except Exception as e:
            logger.warning("Skipping message: %s", str(e))
            continue

    return results, 200


def get_all_traffic_ids():
    """Get all traffic flow id and trace_id from Kafka."""
    results = []
    for msg in kafka_wrapper.messages():
        if msg is None:
            break
        try:
            data = json.loads(msg.value.decode("utf-8"))
            if data.get("type") == "traffic_flow":
                payload = data["payload"]
                results.append({
                    "event_id": payload.get("road_id"),
                    "trace_id": payload.get("trace_id")
                })
        except Exception as e:
            logger.warning("Skipping message: %s", str(e))
            continue

    return results, 200

# Setup Connexion app
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("analyzer.yml", base_path="/analyzer", strict_validation=True, validate_responses=True)

if "CORS_ALLOW_ALL" in os.environ and os.environ["CORS_ALLOW_ALL"] == "yes":
    app.add_middleware(
        CORSMiddleware,
        position=MiddlewarePosition.BEFORE_EXCEPTION,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

if __name__ == "__main__":
    app.run(port=8200, host="0.0.0.0")