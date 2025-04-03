"""Processing Service — Periodically aggregates event stats from the storage service and consumes messages from Kafka."""

from connexion import FlaskApp
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
import json
import os
from datetime import datetime
import yaml
import logging
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler
import httpx
import pytz
from threading import Thread
from kafka_wrapper.kafka_client import KafkaWrapper

# Load app configuration
with open('./config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# Load logging configuration
with open("./config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

STATS_FILE_PATH = app_config['stats']['filename']
STORAGE_SERVICE_URL = app_config['storage']['url']
KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']

def read_stats():
    if os.path.exists(STATS_FILE_PATH):
        with open(STATS_FILE_PATH, 'r') as file:
            return json.load(file)
    else:
        default_stats = {
            "num_air_quality_events": 0,
            "max_air_quality_index": 0,
            "num_traffic_flow_events": 0,
            "max_vehicle_count": 0,
            "last_updated": "1970-01-01T00:00:00Z"
        }
        write_stats(default_stats)
        return default_stats

def write_stats(stats):
    with open(STATS_FILE_PATH, 'w') as file:
        json.dump(stats, file, indent=4)

def get_stats():
    logger.info("Received request for /stats")
    stats = read_stats()
    logger.debug(f"Current stats: {stats}")
    logger.info("Request for /stats completed.")
    return stats, 200

def update_stats_with_event(event_data):
    """Update statistics based on a single Kafka event"""
    stats = read_stats()
    
    event_type = event_data.get("type")
    payload = event_data.get("payload", {})
    
    if event_type == "air_quality":
        stats["num_air_quality_events"] += 1
        air_quality_index = payload.get("air_quality_index", 0)
        stats["max_air_quality_index"] = max(stats["max_air_quality_index"], air_quality_index)
    
    elif event_type == "traffic_flow":
        stats["num_traffic_flow_events"] += 1
        vehicle_count = payload.get("vehicle_count", 0)
        stats["max_vehicle_count"] = max(stats["max_vehicle_count"], vehicle_count)
    
    stats["last_updated"] = datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    write_stats(stats)

def process_kafka_messages():
    """Process messages directly from Kafka"""
    logger.info("Starting Kafka consumer for direct event processing")
    
    try:
        # Use the configuration variables instead of hardcoded values
        kafka_wrapper = KafkaWrapper(KAFKA_HOST, TOPIC_NAME)
        
        # Use the messages() method to get the message iterator
        for message in kafka_wrapper.messages():
            try:
                message_str = message.value.decode('utf-8')
                message_data = json.loads(message_str)
                logger.debug(f"Processing Kafka message directly: {message_data}")
                
                # Update stats with this single event
                update_stats_with_event(message_data)
                
                # Commit offset
                kafka_wrapper.consumer.commit_offsets()
                
            except Exception as error:
                logger.error(f"Error processing Kafka message: {str(error)}", exc_info=True)
                
    except Exception as error:
        logger.critical(f"Critical Kafka consumer error: {str(error)}", exc_info=True)
        # Add reconnection logic here if needed

def populate_stats():
    """Fetch events from storage service and update stats (legacy HTTP-based method)"""
    logger.info("Periodic HTTP-based processing has started.")

    stats = read_stats()
    last_updated_timestamp = stats["last_updated"]

    try:
        last_updated = datetime.strptime(last_updated_timestamp, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        logger.warning(f"Invalid timestamp format in stats: {last_updated_timestamp}, defaulting to epoch.")
        last_updated = datetime(1970, 1, 1, tzinfo=pytz.utc)

    now = datetime.now(pytz.utc)
    start_timestamp = last_updated.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        # Fetch air quality events
        air_response = httpx.get(f"{STORAGE_SERVICE_URL}/city/airquality?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}")
        if air_response.status_code == 200:
            air_events = air_response.json()
            logger.info(f"Received {len(air_events)} new air quality events")
        else:
            logger.error(f"Error retrieving air quality events: {air_response.status_code}")
            air_events = []

        # Fetch traffic flow events
        traffic_response = httpx.get(f"{STORAGE_SERVICE_URL}/city/trafficflow?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}")
        if traffic_response.status_code == 200:
            traffic_events = traffic_response.json()
            logger.info(f"Received {len(traffic_events)} new traffic flow events")
        else:
            logger.error(f"Error retrieving traffic flow events: {traffic_response.status_code}")
            traffic_events = []

        # Update stats
        stats["num_air_quality_events"] += len(air_events)
        if air_events:
            max_air_index = max([event['air_quality_index'] for event in air_events])
            stats["max_air_quality_index"] = max(stats["max_air_quality_index"], max_air_index)

        stats["num_traffic_flow_events"] += len(traffic_events)
        if traffic_events:
            max_vehicle_count = max([event['vehicle_count'] for event in traffic_events])
            stats["max_vehicle_count"] = max(stats["max_vehicle_count"], max_vehicle_count)

        stats["last_updated"] = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        write_stats(stats)
        logger.info("Periodic HTTP-based processing has ended.")

    except Exception as e:
        logger.error(f"Error during periodic processing: {e}")

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['interval'])
    sched.start()

def start_kafka_consumer():
    """Start the Kafka consumer in a separate thread"""
    kafka_thread = Thread(target=process_kafka_messages)
    kafka_thread.daemon = True
    kafka_thread.start()
    logger.info("Kafka consumer thread started")

# Initialize app
app = FlaskApp(__name__)

if os.getenv("CORS_ALLOW_ALL") == "yes":
    app.add_middleware(
        CORSMiddleware,
        position=MiddlewarePosition.BEFORE_EXCEPTION,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.add_api("processing.yaml", base_path="/processing", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    # Initialize the scheduler for HTTP-based processing
    init_scheduler()
    
    # Start the Kafka consumer for direct event processing
    start_kafka_consumer()
    
    app.run(port=8100, host="0.0.0.0")