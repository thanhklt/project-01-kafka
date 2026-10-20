from confluent_kafka import Consumer, KafkaException

from config.settings import load_settings

CONFIG = load_settings("config/config.yml")
GROUP_ID = "local_kafka_to_mongo"
TOPIC_NAME = "product_view"

# Config
CONSUME_CONFIG = {
    "group.id": GROUP_ID,
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
    "allow.auto.create.topics": False,
    "auto.offset.reset": "earliest"
}