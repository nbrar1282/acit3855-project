import connexion
import httpx
import yaml
import uuid
import logging.config
from connexion import NoContent
import time
from pykafka import KafkaClient
import json

logging.Formatter.converter = time.gmtime

# Load logging configuration
with open("./config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)


logger = logging.getLogger("basicLogger")  # Get the logger

# Load configuration from YAML file
with open("./config/app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']

# Connect to Kafka
client = KafkaClient(hosts=KAFKA_HOST)
topic = client.topics[str.encode(TOPIC_NAME)]
producer = topic.get_sync_producer()

# Extract event store URLs based on new structure
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
    body = add_trace_id(body)  # Ensure trace_id is set
    body["event_type"] = "air_quality"

    logger.info(f"Received air_quality event with trace_id {body['trace_id']}")  # Log received event

    try:
        msg = {
            "type": "air_quality",
            "datetime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "payload": body
        }
        producer.produce(json.dumps(msg).encode("utf-8"))  # Produce message to Kafka
        logger.info(f"Produced air_quality event to Kafka (trace_id: {body['trace_id']})")
    except Exception as e:
        logger.error(f"Failed to send air_quality event to Kafka: {str(e)}")
        return NoContent, 500

    return NoContent, 201
def log_traffic_flow_event(body):
    """Sends traffic flow event to Kafka."""
    body = add_trace_id(body)  # Ensure trace_id is set
    body["event_type"] = "traffic_flow"

    logger.info(f"Received traffic_flow event with trace_id {body['trace_id']}")  # Log received event

    try:
        msg = {
            "type": "traffic_flow",
            "datetime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "payload": body
        }
        producer.produce(json.dumps(msg).encode("utf-8"))  # Produce message to Kafka
        logger.info(f"Produced traffic_flow event to Kafka (trace_id: {body['trace_id']})")
    except Exception as e:
        logger.error(f"Failed to send traffic_flow event to Kafka: {str(e)}")
        return NoContent, 500

    return NoContent, 201  



app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("lab1.yaml", base_path="/receiver", strict_validation=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0") # nosec
