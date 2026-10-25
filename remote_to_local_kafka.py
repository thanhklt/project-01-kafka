import threading
from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition

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


def remote_to_local_kafka(worker_id, stop):
    total_count = 0
    batch_size = 500

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

    # Gọi consumer
    try:
        while not stop.is_set():

            # Thông số của mỗi lần consumer:
            success_count = 0
            failed_messages = []

            messages = consumer.consume(
                num_messages=batch_size,
                timeout=1.0
            )

            if len(messages) == 0:
                continue  # Chưa có message, tiếp tục chờ

            for message in messages:
                if message.error():
                    raise KafkaException(message.error())

            def on_delivery(err, msg):
                nonlocal success_count
                nonlocal total_count

                if err is None:
                    success_count += 1
                    total_count += 1
                else:
                    failed_messages.append((msg,err))
 
            next_offsets = {}
            for message in messages:
                producer.produce(
                    topic=TOPIC_NAME,
                    value=message.value(),
                    on_delivery=on_delivery,
                )

                partition_key = (message.topic(), message.partition())
                next_offsets[partition_key] = message.offset() + 1

            # Gom hết msg còn sót lại
            remaining = producer.flush(timeout=30)

            if failed_messages:
                raise KafkaException(f"Có {len(failed_messages)} message gửi thất bại. Lỗi đầu tiên: {failed_messages[0][1]}")

            if remaining > 0:
                raise KafkaException(f"Có {remaining} message gửi thất bại. Lỗi đầu tiên: {failed_messages[0][1]}")
            
            offsets = []
            for (topic, partition), offset in next_offsets.items():
                offsets.append(TopicPartition(topic, partition, offset))

            consumer.commit(
                offsets=offsets, 
                asynchronous=False
            )



            print(
                f"Worker {worker_id} | "
                f"Batch {len(messages)} message | "
                f"Success message {success_count} |"
                f"Tổng đã gửi và commit: {total_count}"
            )
    finally:
        consumer.close()
        producer.flush(timeout=30)
        producer.close()
        
def main() -> None:
    stop = threading.Event()
    threads = []

    for i in range(CONFIG.mirror.consumer_count):
        threads.append(threading.Thread(target=remote_to_local_kafka, args=(i, stop)))
    
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