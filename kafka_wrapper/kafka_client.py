# kafka_wrapper/kafka_client.py

import time
import random
import logging
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException

logger = logging.getLogger("basicLogger")


class KafkaWrapper:
    def __init__(self, hostname, topic, consume_from_start=False):
        self.hostname = hostname
        self.topic = topic.encode() if isinstance(topic, str) else topic
        self.consume_from_start = consume_from_start
        self.client = None
        self.consumer = None
        self.producer = None
        self.connect()

    def connect(self):
        """Infinite loop: will keep trying"""
        while True:
            logger.debug("Trying to connect to Kafka...")
            if self.make_client():
                if self.make_consumer():
                    self.make_producer()
                    break
            # Sleeps for a random amount of time (0.5 to 1.5s)
            time.sleep(random.randint(500, 1500) / 1000)

    def make_client(self):
        """Runs once, makes a client and sets it on the instance."""
        if self.client is not None:
            return True
        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            self.producer = None
            return False

    def make_consumer(self):
        """Runs once, makes a consumer and sets it on the instance."""
        if self.consumer is not None:
            return True
        if self.client is None:
            return False
        try:
            topic = self.client.topics[self.topic]
            self.consumer = topic.get_simple_consumer(
                reset_offset_on_start=self.consume_from_start,
                auto_offset_reset=OffsetType.EARLIEST if self.consume_from_start else OffsetType.LATEST,
                consumer_timeout_ms=1000  # Add this line
            )
            return True
        except KafkaException as e:
            msg = f"Make error when making consumer: {e}"
            logger.warning(msg)
            self.client = None
            self.consumer = None
            return False

    def make_producer(self):
        """Optional: create a producer (used in receiver service)."""
        if self.client is None:
            return False
        try:
            self.producer = self.client.topics[self.topic].get_sync_producer()
            logger.info("Kafka producer created.")
            return True
        except KafkaException as e:
            logger.warning(f"Failed to create producer: {e}")
            self.producer = None
            return False

    def send(self, message: str):
        """Send message to Kafka topic, with retry."""
        if self.producer is None:
            self.make_producer()

        for attempt in range(3):
            try:
                if self.producer is None:
                    logger.warning("No Kafka producer available, retrying connection...")
                    self.connect()
                self.producer.produce(message.encode("utf-8"))
                logger.debug("Kafka message sent.")
                return
            except KafkaException as e:
                logger.warning(f"Kafka send failed (attempt {attempt + 1}): {e}")
                self.producer = None
                time.sleep(0.5 * (attempt + 1))

        logger.error("Kafka send failed after retries.")

    def messages(self):
        """Generator method that catches exceptions in the consumer loop"""
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    yield msg
            except KafkaException as e:
                msg = f"Kafka issue in consumer: {e}"
                logger.warning(msg)
                self.client = None
                self.consumer = None
                self.connect()
