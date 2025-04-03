"""Receiver Service — Handles air quality and traffic flow events, forwards to Kafka."""

import json
import logging
import logging.config
import time
import uuid

import connexion
import yaml
from connexion import NoContent
from pykafka import KafkaClient

from kafka_wrapper.kafka_client import KafkaWrapper

# Use UTC timestamps in logs
logging.Formatter.converter = time.gmtime

# Load logging configuration
with open("./config/log_conf.yml", "r", encoding="utf-8") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load application configuration
with open("./config/app_conf.yml", "r", encoding="utf-8") as f:
    app_config = yaml.safe_load(f.read())

KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']

# Kafka setup
# client = KafkaClient(hosts=KAFKA_HOST)
# topic = client.topics[str.encode(TOPIC_NAME)]
# producer = topic.get_sync_producer()
kafka = KafkaWrapper(KAFKA_HOST, TOPIC_NAME)

# Extract event store URLs
AIR_QUALITY_URL = app_config["events"]["air"]["url"]
TRAFFIC_FLOW_URL = app_config["events"]["traffic"]["url"]


def generate_trace_id():
    """Generates a unique trace_id using UUID4."""
    return str(uuid.uuid4())


def add_trace_id(body):
    """Ensures the event payload contains a trace_id."""
    if body is None:
        body = {}
    if "trace_id" not in body or body["trace_id"] is None:
        body["trace_id"] = generate_trace_id()
    return body


def log_air_quality_event(body):
    """Sends air quality event to Kafka."""
    body = add_trace_id(body)
    body["event_type"] = "air_quality"

    logger.info(f"Received air_quality event with trace_id {body['trace_id']}")  # Log received event

    try:
        msg = {
            "type": "air_quality",
            "datetime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "payload": body
        }
        kafka.send(json.dumps(msg))
        logger.info("Produced air_quality event to Kafka (trace_id: %s)", body["trace_id"])
    except Exception as error:
        logger.error("Failed to send air_quality event to Kafka: %s", str(error))
        return NoContent, 500

    return NoContent, 201


def log_traffic_flow_event(body):
    """Sends traffic flow event to Kafka."""
    body = add_trace_id(body)
    body["event_type"] = "traffic_flow"

    logger.info("Received traffic_flow event with trace_id %s", body["trace_id"])

    try:
        msg = {
            "type": "traffic_flow",
            "datetime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "payload": body
        }
        kafka.send(json.dumps(msg))
        logger.info("Produced traffic_flow event to Kafka (trace_id: %s)", body["trace_id"])
    except Exception as error:
        logger.error("Failed to send traffic_flow event to Kafka: %s", str(error))
        return NoContent, 500

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("lab1.yaml", base_path="/receiver", strict_validation=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")  # nosec
