import threading
import json
from confluent_kafka import Consumer, KafkaException, TopicPartition
from pymongo import MongoClient

from config.settings import load_settings

CONFIG = load_settings("config/config.yml")
GROUP_ID = "local_kafka_to_mongo"
TOPIC_NAME = "product_view"

MONGO_URI = "mongodb://localhost:27017"

# Config
CONSUME_CONFIG = {
    "group.id": GROUP_ID,
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
    "allow.auto.create.topics": False,
    "auto.offset.reset": "earliest"
}

def local_kafka_to_mongo(worker_id, stop):
    count = 0
    batch_size = 500

    consumer_info = CONFIG.destination.to_client_config()
    consumer_info.update(
        CONSUME_CONFIG
    )

    consumer = Consumer(consumer_info)
    consumer.subscribe([TOPIC_NAME])
    
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client["kafka"][TOPIC_NAME]

    # Gọi poll()
    try:
        while not stop.is_set():
            messages = consumer.consume(
                num_messages=batch_size,
                timeout=1.0)
            
            count += batch_size

            if len(messages) == 0:
                continue  # Chưa có message, tiếp tục chờ

            for message in messages:
                if message.error():
                    raise KafkaException(message.error())

            payloads = []
            next_offsets = {}
            for message in messages:
                payload = json.loads(message.value())
                if not isinstance(payload, dict):
                    raise ValueError(
                        f"Payload phải là JSON object: "
                        f"partition={message.partition()}, "
                        f"offset={message.offset()}"
                    )

                payloads.append(payload)

                partition_key = (
                    message.topic(),
                    message.partition(),
                )
                next_offsets[partition_key] = max(
                    next_offsets.get(partition_key, 0),
                    message.offset() + 1,
                )


            collection.insert_many(payloads)
            # Phân partition của topic dựa trên batch_size
            offsets = [
                TopicPartition(topic, partition, offset)
                for (topic, partition), offset
                in next_offsets.items()
            ]

            committed = consumer.commit(
                offsets=offsets,
                asynchronous=False,
            )

            print(
                f"Worker {worker_id} | "
                f"Batch {len(messages)} message | "
                f"Tổng đã gửi và commit: {count}"
            )

    finally:
        consumer.close()
        mongo_client.close()

        
def main() -> None:
    stop = threading.Event()
    threads = []

    for i in range(CONFIG.mirror.consumer_count):
        threads.append(threading.Thread(target=local_kafka_to_mongo, args=(i, stop)))
    
    for thread in threads:
        thread.start()

    try:
        while any(thread.is_alive() for thread in threads):
            stop.wait(0.5)
    except KeyboardInterrupt:
        stop.set()
    finally:
        for thread in threads:
            thread.join()

if __name__ == "__main__":
    main()