import time
import json
import os
from datetime import datetime
import httpx
import connexion
import yaml

app = connexion.FlaskApp(__name__, specification_dir="./")
app.add_api("openapi.yml", base_path="/consistency_check")

with open("config/app_conf.yml", "r") as f:
    APP_CONF = yaml.safe_load(f)

DATA_FILE = APP_CONF["datastore"]["filepath"]

def run_consistency_checks():
    start_time = time.time()

    processing_stats = httpx.get(f'{APP_CONF["processing"]["url"]}/stats').json()
    analyzer_stats = httpx.get(f'{APP_CONF["analyzer"]["url"]}/stats').json()
    storage_counts = httpx.get(f'{APP_CONF["storage"]["url"]}/counts').json()

    analyzer_air_ids = httpx.get(f'{APP_CONF["analyzer"]["url"]}/ids/air').json()
    analyzer_traffic_ids = httpx.get(f'{APP_CONF["analyzer"]["url"]}/ids/traffic').json()

    storage_air_ids = httpx.get(f'{APP_CONF["storage"]["url"]}/ids/air').json()
    storage_traffic_ids = httpx.get(f'{APP_CONF["storage"]["url"]}/ids/traffic').json()

    analyzer_ids = analyzer_air_ids + analyzer_traffic_ids
    storage_ids = storage_air_ids + storage_traffic_ids

    analyzer_set = {(e["trace_id"], e["event_id"]) for e in analyzer_ids}
    storage_set = {(e["trace_id"], e["event_id"]) for e in storage_ids}

    missing_in_db = [dict(trace_id=t[0], id=t[1], type="unknown") for t in analyzer_set - storage_set]
    missing_in_queue = [dict(trace_id=t[0], id=t[1], type="unknown") for t in storage_set - analyzer_set]

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

    duration_ms = int((time.time() - start_time) * 1000)
    return {"processing_time_ms": duration_ms}, 200

def get_checks():
    if not os.path.exists(DATA_FILE):
        return {"message": "No consistency check has been run."}, 404

    with open(DATA_FILE, "r") as f:
        data = json.load(f)
    return data, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8300)