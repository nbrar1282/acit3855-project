# kafka_wrapper/kafka_client.py
import time
import random
import logging
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import KafkaException

logger = logging.getLogger("basicLogger")

class KafkaWrapper:
    def __init__(self, hostname, topic_name: str):
        self.hostname = hostname
        self.topic_name = topic_name.encode()
        self.client = None
        self.producer = None
        self.consumer = None
        self.connect()

    def connect(self):
        while True:
            logger.info("Attempting Kafka connection...")
            if self.make_client():
                if self.make_producer() and self.make_consumer():
                    break
            time.sleep(random.randint(500, 1500) / 1000)

    def make_client(self):
        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client connected.")
            return True
        except KafkaException as e:
            logger.warning(f"Kafka connection failed: {e}")
            return False

    def make_producer(self):
        try:
            self.producer = self.client.topics[self.topic_name].get_sync_producer()
            return True
        except KafkaException as e:
            logger.warning(f"Failed to create producer: {e}")
            return False

    def make_consumer(self):
        try:
            topic = self.client.topics[self.topic_name]
            self.consumer = topic.get_simple_consumer(
                reset_offset_on_start=False,
                auto_offset_reset=OffsetType.LATEST
            )
            return True
        except KafkaException as e:
            logger.warning(f"Failed to create consumer: {e}")
            return False

    def send(self, message: str):
        try:
            self.producer.produce(message.encode("utf-8"))
        except KafkaException as e:
            logger.warning(f"Kafka send failed: {e}")
            self.connect()

    def messages(self):
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    if msg is not None:
                        yield msg
            except KafkaException as e:
                logger.warning(f"Kafka consumer error: {e}")
                self.connect()
