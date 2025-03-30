"""Processing Service — Periodically aggregates event stats from the storage service."""

import json
import logging
import logging.config
import os
import time
from datetime import datetime

import connexion
import httpx
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

# Configure logging to use UTC
logging.Formatter.converter = time.gmtime

# Load logging config
with open('./config/log_conf.yml', 'r', encoding='utf-8') as config_file:
    LOG_CONFIG = yaml.safe_load(config_file.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load application config
with open('./config/app_conf.yml', 'r', encoding='utf-8') as config_file:
    app_config = yaml.safe_load(config_file.read())

STORAGE_SERVICE_URL = app_config['storage']['url']
STATS_FILE = app_config['stats']['filename']

# Default stats structure
DEFAULT_STATS = {
    "num_air_quality_events": 0,
    "max_air_quality_index": 0,
    "num_traffic_flow_events": 0,
    "max_vehicle_count": 0,
    "last_updated": "1970-01-01T00:00:00Z"
}


def populate_stats():
    """Periodically fetch data and update local statistics."""
    logger.info("Starting periodic statistics processing.")

    # Load previous stats or start fresh
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'r', encoding='utf-8') as stats_file:
            stats = json.load(stats_file)
    else:
        stats = DEFAULT_STATS.copy()

    last_updated = stats.get("last_updated", "1970-01-01T00:00:00Z")
    current_time = datetime.utcnow().isoformat() + "Z"

    try:
        air_resp = httpx.get(
            f"{STORAGE_SERVICE_URL}/city/airquality",
            params={"start_timestamp": last_updated, "end_timestamp": current_time}
        )

        traffic_resp = httpx.get(
            f"{STORAGE_SERVICE_URL}/city/trafficflow",
            params={"start_timestamp": last_updated, "end_timestamp": current_time}
        )

        if air_resp.status_code == 200 and traffic_resp.status_code == 200:
            air_events = air_resp.json()
            traffic_events = traffic_resp.json()

            logger.info("Received %d air quality events.", len(air_events))
            logger.info("Received %d traffic flow events.", len(traffic_events))

            stats["num_air_quality_events"] += len(air_events)
            stats["num_traffic_flow_events"] += len(traffic_events)

            if air_events:
                stats["max_air_quality_index"] = max(
                    stats["max_air_quality_index"],
                    max(event["air_quality_index"] for event in air_events)
                )
            if traffic_events:
                stats["max_vehicle_count"] = max(
                    stats["max_vehicle_count"],
                    max(event["vehicle_count"] for event in traffic_events)
                )

            stats["last_updated"] = current_time

            with open(STATS_FILE, 'w', encoding='utf-8') as stats_file:
                json.dump(stats, stats_file, indent=4)

            logger.debug("Updated stats: %s", json.dumps(stats, indent=4))
        else:
            logger.error("Failed to fetch event data from storage service.")

    except Exception as error:
        logger.error("An error occurred during stats processing: %s", str(error))

    logger.info("Periodic statistics processing completed.")


def init_scheduler():
    """Starts the background scheduler to periodically call populate_stats."""
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        populate_stats,
        'interval',
        seconds=app_config['scheduler']['interval']
    )
    scheduler.start()


def get_stats():
    """Handles GET requests to fetch current stats."""
    logger.info("GET /stats request received.")

    if not os.path.exists(STATS_FILE):
        logger.error("Statistics file does not exist.")
        return {"message": "Statistics do not exist"}, 404

    with open(STATS_FILE, 'r', encoding='utf-8') as stats_file:
        stats = json.load(stats_file)

    logger.debug("Returning statistics: %s", json.dumps(stats, indent=4))
    logger.info("GET /stats request processed successfully.")

    return stats, 200


# Create the app and configure middleware
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("processing.yaml", base_path="/processing", strict_validation=True, validate_responses=True)

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
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")
