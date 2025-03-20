import connexion
import yaml
import logging.config
import json
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import os
import time
import httpx
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

logging.Formatter.converter = time.gmtime


# Load logging configuration
with open('./config/log_conf.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load app configuration
with open('./config/app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

STORAGE_SERVICE_URL = app_config['storage']['url']
STATS_FILE = app_config['stats']['filename']

# Initialize default stats
DEFAULT_STATS = {
    "num_air_quality_events": 0,
    "max_air_quality_index": 0,
    "num_traffic_flow_events": 0,
    "max_vehicle_count": 0,
    "last_updated": "1970-01-01T00:00:00Z"
}

def populate_stats():
    """Periodically fetches data from the storage service and updates statistics."""
    logger.info("Starting periodic statistics processing.")

    # Load current stats from file or initialize defaults
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'r') as f:
            stats = json.load(f)
    else:
        stats = DEFAULT_STATS.copy()

    last_updated = stats.get("last_updated", "1970-01-01T00:00:00Z")
    current_time = datetime.utcnow().isoformat() + "Z"

    try:
        # Fetch Air Quality Events
        air_quality_response = httpx.get(
            f"{STORAGE_SERVICE_URL}/city/airquality",
            params={"start_timestamp": last_updated, "end_timestamp": current_time}
        )

        # Fetch Traffic Flow Events
        traffic_flow_response = httpx.get(
            f"{STORAGE_SERVICE_URL}/city/trafficflow",
            params={"start_timestamp": last_updated, "end_timestamp": current_time}
        )

        if air_quality_response.status_code == 200 and traffic_flow_response.status_code == 200:
            air_quality_events = air_quality_response.json()
            traffic_flow_events = traffic_flow_response.json()

            logger.info(f"Received {len(air_quality_events)} air quality events.")
            logger.info(f"Received {len(traffic_flow_events)} traffic flow events.")

            # Update cumulative event counts
            stats["num_air_quality_events"] += len(air_quality_events)
            stats["num_traffic_flow_events"] += len(traffic_flow_events)

            # Update maximum values
            if air_quality_events:
                stats["max_air_quality_index"] = max(
                    stats["max_air_quality_index"],
                    max(event["air_quality_index"] for event in air_quality_events)
                )

            if traffic_flow_events:
                stats["max_vehicle_count"] = max(
                    stats["max_vehicle_count"],
                    max(event["vehicle_count"] for event in traffic_flow_events)
                )

            # Update the last updated timestamp
            stats["last_updated"] = current_time

            # Save updated stats
            with open(STATS_FILE, 'w') as f:
                json.dump(stats, f, indent=4)

            logger.debug(f"Updated statistics: {json.dumps(stats, indent=4)}")
        else:
            logger.error("Error fetching data from the storage service.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

    logger.info("Periodic statistics processing completed.")


def init_scheduler():
    """Initializes the background scheduler."""
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['interval'])
    sched.start()


def get_stats():
    """Handles GET requests for statistics."""
    logger.info("GET /stats request received.")

    if not os.path.exists(STATS_FILE):
        logger.error("Statistics file does not exist.")
        return {"message": "Statistics do not exist"}, 404

    with open(STATS_FILE, 'r') as f:
        stats = json.load(f)

    logger.debug(f"Returning statistics: {json.dumps(stats, indent=4)}")
    logger.info("GET /stats request processed successfully.")

    return stats, 200


# Create the Flask app using Connexion
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("processing.yaml", strict_validation=True, validate_responses=True)
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    )

if __name__ == "__main__":
    init_scheduler()  # Start periodic processing
    app.run(port=8100, host="0.0.0.0")
