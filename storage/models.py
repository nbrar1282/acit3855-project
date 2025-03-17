from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base, mapped_column
import os
import yaml

# Load database configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_CONF_PATH = os.path.join(BASE_DIR, "./config/app_conf.yml")

with open(APP_CONF_PATH, "r") as f:
    app_config = yaml.safe_load(f.read())

DB_USER = app_config["datastore"]["user"]
DB_PASSWORD = app_config["datastore"]["password"]
DB_HOST = app_config["datastore"]["hostname"]
DB_PORT = app_config["datastore"]["port"]
DB_NAME = app_config["datastore"]["db"]

# Define MySQL database connection string
DATABASE_URI = f"mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(DATABASE_URI)
SessionLocal = sessionmaker(bind=engine)

# Define the base class for models
Base = declarative_base()

# Air Quality Model
class AirQualityEvent(Base):
    __tablename__ = "air_quality_events"
    
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    trace_id = mapped_column(String(36), nullable=False)
    sensor_id = mapped_column(String(255), nullable=False)  # Added length (VARCHAR(255))
    air_quality_index = mapped_column(Integer, nullable=False)
    recorded_at = mapped_column(DateTime, nullable=False)
    zone_id = mapped_column(String(255), nullable=False)  # Added length (VARCHAR(255))
    date_created = mapped_column(DateTime, default=func.now())

    def to_dict(self):
        return {
            "id": self.id,
            "trace_id": self.trace_id,
            "sensor_id": self.sensor_id,
            "air_quality_index": self.air_quality_index,
            "recorded_at": self.recorded_at.isoformat() if self.recorded_at else None,
            "zone_id": self.zone_id,
            "date_created": self.date_created.isoformat() if self.date_created else None
        }


# Traffic Flow Model
class TrafficFlowEvent(Base):
    __tablename__ = "traffic_flow_events"
    
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    trace_id = mapped_column(String(36), nullable=False)
    road_id = mapped_column(String(255), nullable=False)  # Added length (VARCHAR(255))
    vehicle_count = mapped_column(Integer, nullable=False)
    time_registered = mapped_column(DateTime, nullable=False)
    average_speed = mapped_column(Integer, nullable=False)
    date_created = mapped_column(DateTime, default=func.now())

    def to_dict(self):
        return {
            "id": self.id,
            "trace_id": self.trace_id,
            "road_id": self.road_id,
            "vehicle_count": self.vehicle_count,
            "time_registered": self.time_registered.isoformat() if self.time_registered else None,
            "average_speed(in km/h)": self.average_speed,
            "date_created": self.date_created.isoformat() if self.date_created else None
        }


# Function to create tables
def init_db():
    Base.metadata.create_all(engine)

# Function to drop tables
def drop_db():
    """Drops all tables in the database."""
    Base.metadata.drop_all(engine)
    print("All tables dropped successfully!")
