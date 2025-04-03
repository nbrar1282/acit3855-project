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
from kafka_wrapper.kafka_client import KafkaWrapper

# Use UTC timestamps in logs
logging.Formatter.converter = time.gmtime

# Load logging configuration
with open("./config/log_conf.yml", "r", encoding="utf-8") as config_file:
    LOG_CONFIG = yaml.safe_load(config_file.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load application configuration
with open("./config/app_conf.yml", "r", encoding="utf-8") as config_file:
    app_config = yaml.safe_load(config_file.read())

# Kafka connection details
KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']


def get_kafka_messages():
    """Helper function to get all messages from Kafka topic."""
    kafka_wrapper = KafkaWrapper(KAFKA_HOST, TOPIC_NAME)
    messages = []
    
    # Use the messages() method from KafkaWrapper
    for msg in kafka_wrapper.messages():
        if msg is None:
            break
            
        try:
            data = json.loads(msg.value.decode("utf-8"))
            messages.append(data)
        except (json.JSONDecodeError, AttributeError) as error:
            logger.warning("Skipping malformed message: %s", str(error))
            continue
            
        # Break after consuming all available messages
        # This is needed because messages() is an infinite iterator
        if not kafka_wrapper.consumer.has_message_available():
            break
            
    return messages


def get_event_by_index(event_type: str, index: int) -> Tuple[dict, int]:
    """Retrieve a specific event by index for a given event type."""
    try:
        messages = get_kafka_messages()
        
        # Filter for the specific event type
        filtered_events = [msg for msg in messages if msg.get("type") == event_type]
        
        if index < len(filtered_events):
            logger.info("Returning %s event at index %d", event_type, index)
            return filtered_events[index]["payload"], 200
        else:
            logger.warning("No %s event found at index %d", event_type, index)
            return {"message": f"No {event_type} event found at index {index}"}, 404
    
    except Exception as e:
        logger.error("Error retrieving event by index: %s", str(e))
        return {"message": f"Error retrieving {event_type} event: {str(e)}"}, 500


def get_air_quality_event(index: int) -> Tuple[dict, int]:
    """Handle request to get an air quality event by index."""
    return get_event_by_index("air_quality", index)


def get_traffic_flow_event(index: int) -> Tuple[dict, int]:
    """Handle request to get a traffic flow event by index."""
    return get_event_by_index("traffic_flow", index)


def get_event_stats() -> Tuple[Any, int]:
    """Return count of air quality and traffic flow events currently in Kafka."""
    try:
        messages = get_kafka_messages()
        
        air_quality_count = sum(1 for msg in messages if msg.get("type") == "air_quality")
        traffic_flow_count = sum(1 for msg in messages if msg.get("type") == "traffic_flow")
        
        stats = {
            "num_air_quality_events": air_quality_count,
            "num_traffic_flow_events": traffic_flow_count
        }
        
        logger.info("Returning event stats: %s", stats)
        return jsonify(stats), 200
    
    except Exception as e:
        logger.error("Error retrieving event stats: %s", str(e))
        return {"message": f"Error retrieving event stats: {str(e)}"}, 500


def get_all_air_ids():
    """Get all air quality id and trace_id from Kafka."""
    try:
        messages = get_kafka_messages()
        
        results = []
        for msg in messages:
            if msg.get("type") == "air_quality":
                payload = msg["payload"]
                results.append({
                    "event_id": payload.get("sensor_id"),  
                    "trace_id": payload.get("trace_id")
                })
        
        return results, 200
    
    except Exception as e:
        logger.error("Error retrieving air quality IDs: %s", str(e))
        return {"message": f"Error retrieving air quality IDs: {str(e)}"}, 500


def get_all_traffic_ids():
    """Get all traffic flow id and trace_id from Kafka."""
    try:
        messages = get_kafka_messages()
        
        results = []
        for msg in messages:
            if msg.get("type") == "traffic_flow":
                payload = msg["payload"]
                results.append({
                    "event_id": payload.get("road_id"),  
                    "trace_id": payload.get("trace_id")
                })
        
        return results, 200
    
    except Exception as e:
        logger.error("Error retrieving traffic flow IDs: %s", str(e))
        return {"message": f"Error retrieving traffic flow IDs: {str(e)}"}, 500


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