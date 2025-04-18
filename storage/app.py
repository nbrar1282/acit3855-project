"""Storage Service — Receives events from Kafka and stores them in the database."""

import json
import logging
import logging.config
import time
from datetime import datetime
from threading import Thread

import connexion
import yaml
from kafka_wrapper.kafka_client import KafkaWrapper  # Uses our wrapper

from models import SessionLocal, AirQualityEvent, TrafficFlowEvent, init_db
from sqlalchemy import select


# Configure logging to use UTC timestamps
logging.Formatter.converter = time.gmtime

# Load logging configuration
with open('./config/log_conf.yml', "r", encoding="utf-8") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load application configuration
with open('./config/app_conf.yml', "r", encoding="utf-8") as f:
    app_config = yaml.safe_load(f.read())

# Kafka configuration
KAFKA_HOST = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
TOPIC_NAME = app_config['events']['topic']



# Initialize DB
init_db()


def get_air_quality_events(start_timestamp, end_timestamp):
    session = SessionLocal()
    try:
        start = datetime.fromisoformat(start_timestamp.replace("Z", ""))
        end = datetime.fromisoformat(end_timestamp.replace("Z", ""))
        statement = (
            select(AirQualityEvent)
            .where(AirQualityEvent.date_created >= start)
            .where(AirQualityEvent.date_created < end)
        )
        results = session.execute(statement).scalars().all()
        events = [result.to_dict() for result in results]
        logger.info("Found %d air quality events (start: %s, end: %s)", len(events), start, end)
        return events, 200
    finally:
        session.close()


def get_traffic_flow_events(start_timestamp, end_timestamp):
    session = SessionLocal()
    try:
        start = datetime.fromisoformat(start_timestamp.replace("Z", ""))
        end = datetime.fromisoformat(end_timestamp.replace("Z", ""))
        statement = (
            select(TrafficFlowEvent)
            .where(TrafficFlowEvent.date_created >= start)
            .where(TrafficFlowEvent.date_created < end)
        )
        results = session.execute(statement).scalars().all()
        events = [result.to_dict() for result in results]
        logger.info("Found %d traffic flow events (start: %s, end: %s)", len(events), start, end)
        return events, 200
    finally:
        session.close()


def store_air_quality_event(payload):
    session = SessionLocal()
    try:
        recorded_at = datetime.fromisoformat(payload["recorded_at"].replace("Z", ""))
        event = AirQualityEvent(
            trace_id=payload["trace_id"],
            sensor_id=payload["sensor_id"],
            air_quality_index=payload["air_quality_index"],
            recorded_at=recorded_at,
            zone_id=payload["zone_id"]
        )
        session.add(event)
        session.commit()
        logger.info("Stored air_quality event with trace_id %s", payload["trace_id"])
    except Exception as error:
        logger.error("Error storing air_quality event: %s", str(error))
        session.rollback()
    finally:
        session.close()


def store_traffic_flow_event(payload):
    session = SessionLocal()
    try:
        time_registered = datetime.fromisoformat(payload["time_registered"].replace("Z", ""))
        event = TrafficFlowEvent(
            trace_id=payload["trace_id"],
            road_id=payload["road_id"],
            vehicle_count=payload["vehicle_count"],
            time_registered=time_registered,
            average_speed=payload["average_speed(in km/h)"]
        )
        session.add(event)
        session.commit()
        logger.info("Stored traffic_flow event with trace_id %s", payload["trace_id"])
    except Exception as error:
        logger.error("Error storing traffic_flow event: %s", str(error))
        session.rollback()
    finally:
        session.close()


def process_messages():
    logger.info("Starting Kafka consumer thread...")
    kafka_wrapper = KafkaWrapper(KAFKA_HOST, TOPIC_NAME)
    try:
        for message in kafka_wrapper.messages():
            try:
                message_str = message.value.decode("utf-8")
                message_data = json.loads(message_str)
                logger.info("Kafka message received: %s", message_data)

                payload = message_data.get("payload")
                msg_type = message_data.get("type")

                if msg_type == "air_quality":
                    store_air_quality_event(payload)
                elif msg_type == "traffic_flow":
                    store_traffic_flow_event(payload)
                else:
                    logger.warning(f"Unknown message type: {msg_type}")
                    
                kafka_wrapper.commit()

            except Exception as e:
                logger.error("Message handling error: %s", str(e), exc_info=True)
    except Exception as e:
        logger.critical("Kafka consumer crashed: %s", str(e), exc_info=True)


def setup_kafka_thread():
    kafka_thread = Thread(target=process_messages)
    kafka_thread.daemon = True
    kafka_thread.start()


def get_event_counts():
    session = SessionLocal()
    try:
        air_count = session.query(AirQualityEvent).count()
        traffic_count = session.query(TrafficFlowEvent).count()
        return {"air": air_count, "traffic": traffic_count}, 200
    except Exception as e:
        logger.error("Error getting event counts: %s", str(e))
        return {"message": "Internal server error"}, 500
    finally:
        session.close()


def get_air_ids():
    session = SessionLocal()
    try:
        results = session.query(AirQualityEvent.sensor_id, AirQualityEvent.trace_id).all()
        events = [{"event_id": r[0], "trace_id": r[1]} for r in results]
        return events, 200
    except Exception as e:
        logger.error("Error getting air IDs: %s", str(e))
        return {"message": "Internal server error"}, 500
    finally:
        session.close()


def get_traffic_ids():
    session = SessionLocal()
    try:
        results = session.query(TrafficFlowEvent.road_id, TrafficFlowEvent.trace_id).all()
        events = [{"event_id": r[0], "trace_id": r[1]} for r in results]
        return events, 200
    except Exception as e:
        logger.error("Error getting traffic IDs: %s", str(e))
        return {"message": "Internal server error"}, 500
    finally:
        session.close()


# Create the app
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("lab1.yaml", base_path="/storage", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")