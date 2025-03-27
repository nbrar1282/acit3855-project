import connexion
from datetime import datetime
from connexion import NoContent
import logging.config
import yaml
import os
from models import SessionLocal, AirQualityEvent, TrafficFlowEvent, init_db  # Import models
from sqlalchemy import select
import time
from pykafka import KafkaClient
from pykafka.common import OffsetType

from threading import Thread
import json


logging.Formatter.converter = time.gmtime


# Load logging configuration
with open('./config/log_conf.yml', "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")  # Get the configured logger

with open('./config/app_conf.yml', "r") as f:
    app_config = yaml.safe_load(f.read())

# Extract Kafka connection details
KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']    

# Initialize database tables
init_db()

# Get Air Quality Events
def get_air_quality_events(start_timestamp, end_timestamp):
    """Retrieve air quality events within the date range."""
    session = SessionLocal()
    try:
        # Convert ISO timestamps to datetime objects
        start = datetime.fromisoformat(start_timestamp.replace("Z", ""))
        end = datetime.fromisoformat(end_timestamp.replace("Z", ""))

        # Dynamic SQLAlchemy ORM Query
        statement = (
            select(AirQualityEvent)
            .where(AirQualityEvent.date_created >= start)
            .where(AirQualityEvent.date_created < end)
        )

        # Execute the ORM query
        results = session.execute(statement).scalars().all()

        # Convert ORM objects to dictionaries for JSON response
        events = [result.to_dict() for result in results]

        logger.info(f"Found {len(events)} air quality events (start: {start}, end: {end})")
        return events, 200

    finally:
        session.close()


# Get Traffic Flow Events
def get_traffic_flow_events(start_timestamp, end_timestamp):
    """Retrieve traffic flow events within the date range."""
    session = SessionLocal()
    try:
        # Convert ISO timestamps to datetime objects
        start = datetime.fromisoformat(start_timestamp.replace("Z", ""))
        end = datetime.fromisoformat(end_timestamp.replace("Z", ""))

        # Dynamic SQLAlchemy ORM Query
        statement = (
            select(TrafficFlowEvent)
            .where(TrafficFlowEvent.date_created >= start)
            .where(TrafficFlowEvent.date_created < end)
        )

        # Execute the ORM query
        results = session.execute(statement).scalars().all()

        # Convert to dictionary for JSON response
        events = [result.to_dict() for result in results]

        logger.info(f"Found {len(events)} traffic flow events (start: {start}, end: {end})")
        return events, 200

    finally:
        session.close()


def process_messages():
    """ Process event messages """
    logger.info("Starting Kafka consumer thread...")

    try:
        client = KafkaClient(hosts=KAFKA_HOST)
        topic = client.topics[str.encode(TOPIC_NAME)]
        
        consumer = topic.get_simple_consumer(
            consumer_group=b'event_group',
            reset_offset_on_start=False,
            auto_offset_reset=OffsetType.LATEST
        )

        for msg in consumer:
            try:
                msg_str = msg.value.decode('utf-8')
                msg = json.loads(msg_str)

                logger.info(f"Message: {msg}")

                payload = msg["payload"]

                if msg["type"] == "air_quality":
                    store_air_quality_event(payload)
                elif msg["type"] == "traffic_flow":
                    store_traffic_flow_event(payload)

                consumer.commit_offsets()
            
            except Exception as e:
                logger.error(f"Error processing Kafka message: {str(e)}", exc_info=True)

    except Exception as e:
        logger.critical(f"Critical Kafka error: {str(e)}", exc_info=True)


def store_air_quality_event(payload):
    """Stores an air quality event in the database."""
    session = SessionLocal()
    try:
        recorded_at_dt = datetime.fromisoformat(payload["recorded_at"].replace("Z", ""))

        event = AirQualityEvent(
            trace_id=payload["trace_id"],
            sensor_id=payload["sensor_id"],
            air_quality_index=payload["air_quality_index"],
            recorded_at=recorded_at_dt,
            zone_id=payload["zone_id"]
        )
        session.add(event)
        session.commit()

        logger.info(f"Stored air_quality event with trace_id {payload['trace_id']}")

    except Exception as e:
        logger.error(f"Error storing air_quality event: {str(e)}")
        session.rollback()
    finally:
        session.close()


def store_traffic_flow_event(payload):
    """Stores a traffic flow event in the database."""
    session = SessionLocal()
    try:
        time_registered_dt = datetime.fromisoformat(payload["time_registered"].replace("Z", ""))

        event = TrafficFlowEvent(
            trace_id=payload["trace_id"],
            road_id=payload["road_id"],
            vehicle_count=payload["vehicle_count"],
            time_registered=time_registered_dt,
            average_speed=payload["average_speed(in km/h)"]
        )
        session.add(event)
        session.commit()

        logger.info(f"Stored traffic_flow event with trace_id {payload['trace_id']}")

    except Exception as e:
        logger.error(f"Error storing traffic_flow event: {str(e)}")
        session.rollback()
    finally:
        session.close()


def setup_kafka_thread():
    """Set up a thread to consume Kafka messages continuously."""
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()

# Create the application instance
app = connexion.FlaskApp(__name__, specification_dir='')

app.add_api("lab1.yaml", base_path="/storage", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")
