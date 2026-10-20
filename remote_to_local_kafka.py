import threading
from confluent_kafka import Consumer, Producer, KafkaException

from config.settings import load_settings

CONFIG = load_settings("config/config.yml")
GROUP_ID = "remote_to_local_kafka"
TOPIC_NAME = "product_view"

# Config
CONSUME_CONFIG = {
    "group.id": GROUP_ID,
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
    "allow.auto.create.topics": False,
    "auto.offset.reset": "earliest"
}

PRODUCER_CONFIG = {
    "enable.idempotence": True,
    "acks": "all",
}


def remote_to_local_kafka(worker_id, stop, count):
    consumer_info = CONFIG.source.to_client_config()
    consumer_info.update(
        CONSUME_CONFIG
    )

    producer_info = CONFIG.destination.to_client_config()
    producer_info.update(
        PRODUCER_CONFIG
    )

    consumer = Consumer(consumer_info)
    consumer.subscribe([TOPIC_NAME])
    
    producer = Producer(producer_info)

    # Gọi poll()
    try:
        while not stop.is_set():
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue  # Chưa có message, tiếp tục chờ

            if message.error():
                raise KafkaException(message.error())

            delivery = {
                "confirmed": False, 
                "error": None
            }
            def on_delivery(err, delivered_message):
                delivery["confirmed"] = True
                delivery["error"] = err

            producer.produce(
                topic=TOPIC_NAME,
                value=message.value(),
                on_delivery=on_delivery
            )
            
            remaining = producer.flush(timeout=30) # Gửi hết tin nhắn

            if remaining > 0 or not delivery["confirmed"]:
                raise RuntimeError(
                    f"Worker {worker_id}: chưa xác nhận gửi sang local"
                )

            consumer.commit(message=message, asynchronous=False)

            if delivery["error"] != None:
                raise KafkaException(delivery["error"])
            count += 1
            print(
                f"Đã xử lý {count} message |"
                f"Worker {worker_id} | "
                f"Source partition {message.partition()} | "
                f"Source offset {message.offset()} | "
                f"Đã gửi vào local cluster và commit."
            )
    finally:
        consumer.close()
        producer.flush(timeout=30)
        producer.close()
        
def main() -> None:
    stop = threading.Event()
    count = 0
    threads = []

    for i in range(CONFIG.mirror.consumer_count):
        threads.append(threading.Thread(target=remote_to_local_kafka, args=(i, stop, count)))
    
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