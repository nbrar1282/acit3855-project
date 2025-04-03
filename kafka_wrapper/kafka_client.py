# kafka_wrapper/kafka_client.py

import time
import random
import logging
from threading import Lock
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException

logger = logging.getLogger("basicLogger")


class KafkaWrapper:
    def __init__(self, hostname, topic_name: str, consume_from_start: bool = False):
        """
        :param hostname: Kafka hostname (e.g., 'kafka:9092')
        :param topic_name: Kafka topic name (str or bytes)
        :param consume_from_start: if True, resets offset to start (for analytics)
        """
        self.hostname = hostname
        self.topic_name = topic_name.encode() if isinstance(topic_name, str) else topic_name
        self.consume_from_start = consume_from_start

        self.client = None
        self.producer = None
        self.consumer = None
        self.lock = Lock()  # Thread-safety for producer

        self.connect()

    def connect(self):
        while True:
            logger.info("Attempting Kafka connection...")
            if self.make_client():
                if self.make_producer() and self.make_consumer():
                    logger.info("Kafka connected successfully.")
                    break
            sleep_time = random.randint(500, 1500) / 1000
            logger.warning(f"Kafka connection failed. Retrying in {sleep_time:.2f} seconds.")
            time.sleep(sleep_time)

    def make_client(self):
        if self.client is not None:
            return True
        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created.")
            return True
        except KafkaException as e:
            logger.warning(f"Kafka client creation failed: {e}")
            self.client = None
            return False

    def make_producer(self):
        if self.client is None:
            return False
        try:
            self.producer = self.client.topics[self.topic_name].get_sync_producer()
            logger.info("Kafka producer created.")
            return True
        except KafkaException as e:
            logger.warning(f"Kafka producer creation failed: {e}")
            self.producer = None
            return False

    def make_consumer(self):
        if self.client is None:
            return False
        try:
            topic = self.client.topics[self.topic_name]
            self.consumer = topic.get_simple_consumer(
                reset_offset_on_start=self.consume_from_start,
                auto_offset_reset=OffsetType.EARLIEST if self.consume_from_start else OffsetType.LATEST
            )
            logger.info("Kafka consumer created (from start: %s).", self.consume_from_start)
            return True
        except KafkaException as e:
            logger.warning(f"Kafka consumer creation failed: {e}")
            self.consumer = None
            return False

    def send(self, message: str):
        """Send message via Kafka producer. Reconnect if needed."""
        with self.lock:
            for attempt in range(3):
                try:
                    if self.producer is None:
                        self.connect()
                    self.producer.produce(message.encode("utf-8"))
                    logger.debug("Kafka message sent.")
                    return
                except KafkaException as e:
                    logger.warning(f"Kafka send failed (attempt {attempt + 1}): {e}")
                    self.producer = None
                    time.sleep(0.5 * (attempt + 1))
            logger.error("Kafka send ultimately failed after retries.")

    def messages(self):
        """Generator that yields messages from Kafka with retry logic."""
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    if msg is not None:
                        yield msg
            except KafkaException as e:
                logger.warning(f"Kafka consumer error: {e}")
                self.client = None
                self.consumer = None
                self.connect()
