import connexion
import yaml
import logging.config
import json
import time
from pykafka import KafkaClient
from flask import jsonify
from connexion import NoContent
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

# Configure logging to use UTC timestamps
logging.Formatter.converter = time.gmtime

# Load logging configuration
with open("./config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")  # Get the logger

# Load configuration from YAML file
with open("./config/app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

# Kafka connection details
KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']

# Connect to Kafka
client = KafkaClient(hosts=KAFKA_HOST)
topic = client.topics[str.encode(TOPIC_NAME)]


# def get_event_by_index(event_type, index):
#     """Retrieves an event of a specific type at the given index from Kafka."""
#     with consumer_lock:
#         consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

#         air_quality_events = []
#         traffic_flow_events = []

#         counter = 0
#         for msg in consumer:
#             if msg is None:
#                 break
#             message = msg.value.decode("utf-8")
#             data = json.loads(message)

#             # Categorize event based on type
#             if data["type"] == "air_quality":
#                 air_quality_events.append(data["payload"])
#             elif data["type"] == "traffic_flow":
#                 traffic_flow_events.append(data["payload"])

#         # Select event list based on event type
#         if event_type == "air_quality":
#             events_list = air_quality_events
#         elif event_type == "traffic_flow":
#             events_list = traffic_flow_events
#         else:
#             return {"message": "Invalid event type"}, 400

#         # Check if index exists
#         if index >= len(events_list) or index < 0:
#             return {"message": f"No event found at index {index}"}, 404

#         logger.info(f"Returning {event_type} event at index {index}")
#         return events_list[index], 200

def get_event_by_index(event_type, index):
    """Retrieves an event of a specific type at the given index from Kafka."""
    consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

    counter = 0
    for msg in consumer:
        if msg is None:
            break
        message = msg.value.decode("utf-8")
        data = json.loads(message)

        # Check if the message type matches the requested event type
        if data["type"] == event_type:
            if counter == index:
                logger.info(f"Returning {event_type} event at index {index}")
                return data["payload"], 200
            counter += 1  # Increment counter only for matching events

    # If the index is not found, return 404
    return {"message": f"No {event_type} event found at index {index}"}, 404



def get_air_quality_event(index):
    """Retrieves an air quality event from Kafka by index."""
    return get_event_by_index("air_quality", index)


def get_traffic_flow_event(index):
    """Retrieves a traffic flow event from Kafka by index."""
    return get_event_by_index("traffic_flow", index)


def get_event_stats():
    """Retrieves the number of air quality and traffic flow events in Kafka."""
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

    logger.info(f"Returning event stats: {stats}")
    return jsonify(stats), 200


# Create Flask app with Connexion
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("analyzer.yml", base_path="/analyzer",  strict_validation=True, validate_responses=True)
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    app.run(port=8200, host="0.0.0.0")
