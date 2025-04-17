import os
import json
import logging
import logging.config
import time
from datetime import datetime
from flask import jsonify

import connexion
import yaml
from kafka_wrapper.kafka_client import KafkaWrapper 

# Logging setup
logging.Formatter.converter = time.gmtime
with open("./config/log_conf.yml", "r") as f:
    log_cfg = yaml.safe_load(f)
    logging.config.dictConfig(log_cfg)

logger = logging.getLogger("basicLogger")

# Load config
with open("./config/app_conf.yml", "r") as f:
    app_cfg = yaml.safe_load(f)

KAFKA_HOST = f"{app_cfg['events']['hostname']}:{app_cfg['events']['port']}"
TOPIC_NAME = app_cfg['events']['topic']
DATASTORE_PATH = app_cfg['datastore']['filename']

# Environment thresholds
AIR_QUALITY_MAX = int(os.getenv("AIR_QUALITY_MAX", 100))     # can set uo differnt values for our kafka queue based on this 
VEHICLE_COUNT_MAX = int(os.getenv("VEHICLE_COUNT_MAX", 500)) 

logger.info(f"Anomaly Detector started with thresholds — AIR_QUALITY_MAX: {AIR_QUALITY_MAX}, VEHICLE_COUNT_MAX: {VEHICLE_COUNT_MAX}")

def update_anomalies():
    start = time.time()
    logger.debug("Received request to update anomalies.")

    kafka = KafkaWrapper(KAFKA_HOST, TOPIC_NAME, consume_from_start=True, use_consumer_group=False)
    anomalies = []

    for msg in kafka.messages():
        if msg is None:
            break

        try:
            event = json.loads(msg.value.decode("utf-8"))
            payload = event["payload"]
            event_type = event["type"]
            trace_id = payload.get("trace_id", "unknown")

            if event_type == "air_quality":
                index = payload.get("air_quality_index", 0)
                if index > AIR_QUALITY_MAX:
                    anomalies.append({
                        "event_id": payload["sensor_id"],
                        "trace_id": trace_id,
                        "event_type": "AIR",
                        "anomaly_type": "Too High",
                        "description": f"Detected: {index}; too high (threshold {AIR_QUALITY_MAX})"
                    })
                    logger.debug(f"Anomaly detected [AIR]: {index} > {AIR_QUALITY_MAX}")

            elif event_type == "traffic_flow":
                count = payload.get("vehicle_count", 0)
                if count > VEHICLE_COUNT_MAX:
                    anomalies.append({
                        "event_id": payload["road_id"],
                        "trace_id": trace_id,
                        "event_type": "TRAFFIC",
                        "anomaly_type": "Too High",
                        "description": f"Detected: {count}; too high (threshold {VEHICLE_COUNT_MAX})"
                    })
                    logger.debug(f"Anomaly detected [TRAFFIC]: {count} > {VEHICLE_COUNT_MAX}")

        except Exception as e:
            logger.warning(f"Skipping bad message: {e}")

    # Save anomalies in the file in the data folder
    with open(DATASTORE_PATH, "w") as f:
        json.dump(anomalies, f, indent=4)

    elapsed = int((time.time() - start) * 1000)
    logger.info(f"Anomaly detection completed | processing_time_ms={elapsed} | anomalies_count={len(anomalies)}")

    return {"anomalies_count": len(anomalies)}, 201


def get_anomalies(event_type=None):
    logger.debug(f"GET /anomalies received. Filter: {event_type}")

    if not os.path.exists(DATASTORE_PATH):
        return {"message": "Anomaly datastore not found."}, 404

    try:
        with open(DATASTORE_PATH, "r") as f:
            anomalies = json.load(f)
    except Exception:
        return {"message": "Invalid datastore file."}, 404

    if event_type:
        if event_type not in ["AIR", "TRAFFIC"]:
            return {"message": "Invalid event_type. Must be AIR or TRAFFIC."}, 400
        anomalies = [a for a in anomalies if a["event_type"] == event_type]

    if not anomalies:
        return "", 204

    return anomalies, 200


# Setup Connexion app
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("anomaly.yaml", base_path="/anomaly_detector", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8400, host="0.0.0.0")