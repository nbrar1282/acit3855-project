"""Analyzer Service — Reads Kafka events and exposes APIs for event inspection and stats."""

import json
import logging
import logging.config
import time

import connexion
import yaml
from flask import jsonify
from pykafka import KafkaClient
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

# Use UTC timestamps for logging
logging.Formatter.converter = time.gmtime

# Load logging configuration
with open("./config/log_conf.yml", "r", encoding="utf-8") as config_file:
    LOG_CONFIG = yaml.safe_load(config_file.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load application configuration
with open("./config/app_conf.yml", "r", encoding="utf-8") as config_file:
    app_config = yaml.safe_load(config_file.read())

# Kafka setup
KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']
client = KafkaClient(hosts=KAFKA_HOST)
topic = client.topics[str.encode(TOPIC_NAME)]


def get_event_by_index(event_type, index):
    """Retrieve a specific event by index for a given event type."""
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
    counter = 0

    for msg in consumer:
        if msg is None:
            break
        message = msg.value.decode("utf-8")
        data = json.loads(message)

        if data["type"] == event_type:
            if counter == index:
                logger.info("Returning %s event at index %d", event_type, index)
                return data["payload"], 200
            counter += 1

    logger.warning("No %s event found at index %d", event_type, index)
    return {"message": f"No {event_type} event found at index {index}"}, 404


def get_air_quality_event(index):
    """Handle request to get an air quality event by index."""
    return get_event_by_index("air_quality", index)


def get_traffic_flow_event(index):
    """Handle request to get a traffic flow event by index."""
    return get_event_by_index("traffic_flow", index)


def get_event_stats():
    """Return count of air quality and traffic flow events currently in Kafka."""
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)
    air_quality_count = 0
    traffic_flow_count = 0

    for msg in consumer:
        if msg is None:
            break
        message = msg.value.decode("utf-8")
        data = json.loads(message)

        if data["type"] == "air_quality":
            air_quality_count += 1
        elif data["type"] == "traffic_flow":
            traffic_flow_count += 1

    stats = {
        "num_air_quality_events": air_quality_count,
        "num_traffic_flow_events": traffic_flow_count
    }

    logger.info("Returning event stats: %s", stats)
    return jsonify(stats), 200


# Setup Flask app using Connexion
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("analyzer.yml", base_path="/analyzer", strict_validation=True, validate_responses=True)
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