import time
import json
import os
from datetime import datetime
import httpx
import connexion
import yaml
import logging
import logging.config

# Load logging config
with open("config/log_conf.yml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger("basicLogger")

# Load app config
with open("config/app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f)

DATA_FILE = APP_CONF["datastore"]["filepath"]

app = connexion.FlaskApp(__name__, specification_dir="./")
app.add_api("openapi.yml", base_path="/consistency_check")

def run_consistency_checks():
    start_time = time.time()
    logger.info("Running consistency checks...")

    try:
        processing_stats = httpx.get(f'{APP_CONF["processing"]["url"]}/stats').json()
        analyzer_stats = httpx.get(f'{APP_CONF["analyzer"]["url"]}/stats').json()
        storage_counts = httpx.get(f'{APP_CONF["storage"]["url"]}/counts').json()

        analyzer_air_ids = httpx.get(f'{APP_CONF["analyzer"]["url"]}/ids/air').json()
        analyzer_traffic_ids = httpx.get(f'{APP_CONF["analyzer"]["url"]}/ids/traffic').json()

        storage_air_ids = httpx.get(f'{APP_CONF["storage"]["url"]}/ids/air').json()
        storage_traffic_ids = httpx.get(f'{APP_CONF["storage"]["url"]}/ids/traffic').json()

        logger.debug("Fetched stats and IDs from all services.")

        analyzer_ids = [
            {"trace_id": e["trace_id"], "event_id": e["event_id"], "type": "air"} for e in analyzer_air_ids
        ] + [
            {"trace_id": e["trace_id"], "event_id": e["event_id"], "type": "traffic"} for e in analyzer_traffic_ids
        ]

        storage_ids = [
            {"trace_id": e["trace_id"], "event_id": e["event_id"], "type": "air"} for e in storage_air_ids
        ] + [
            {"trace_id": e["trace_id"], "event_id": e["event_id"], "type": "traffic"} for e in storage_traffic_ids
        ]

        analyzer_set = {(e["trace_id"], e["event_id"], e["type"]) for e in analyzer_ids}
        storage_set = {(e["trace_id"], e["event_id"], e["type"]) for e in storage_ids}

        missing_in_db = [{"trace_id": t[0], "event_id": t[1], "type": t[2]} for t in analyzer_set - storage_set]
        missing_in_queue = [{"trace_id": t[0], "event_id": t[1], "type": t[2]} for t in storage_set - analyzer_set]

        output = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "counts": {
                "processing": processing_stats,
                "queue": analyzer_stats,
                "db": storage_counts
            },
            "missing_in_db": missing_in_db,
            "missing_in_queue": missing_in_queue
        }

        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, "w") as f:
            json.dump(output, f, indent=4)

        logger.info(f"Consistency check complete. Missing in DB: {len(missing_in_db)}, Missing in Queue: {len(missing_in_queue)}")

    except Exception as e:
        logger.error(f"Error during consistency check: {e}")

    duration_ms = int((time.time() - start_time) * 1000)
    logger.debug(f"Consistency check duration: {duration_ms}ms")
    return {"processing_time_ms": duration_ms}, 200


def get_checks():
    logger.info("GET /consistency_check request received.")
    if not os.path.exists(DATA_FILE):
        logger.warning("No consistency check file found.")
        return {"message": "No consistency check has been run."}, 404

    with open(DATA_FILE, "r") as f:
        data = json.load(f)
        logger.debug("Returning consistency check data.")
    return data, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8300)
