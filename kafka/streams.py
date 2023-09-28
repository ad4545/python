from confluent_kafka import Consumer, KafkaError, Producer
import json

streams_config = {
    # Replace with your Kafka broker address
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group-0',
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

streams_producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer-2'
}

stream_consumer = Consumer(streams_config)
stream_producer = Producer(streams_producer_config)

source_topic = 'stream-topic'

try:
    stream_consumer.subscribe([source_topic])
    while True:
        msg = stream_consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                print(f"Error: {msg.error()}")
        else:
            print(f"Stream message:{msg.value()}")
            transform_msg = msg.value().upper()

            stream_producer.produce(
                'test-topic', key='key', value=transform_msg)
            stream_producer.flush()
except print(0):
    pass


stream_consumer.close()
stream_producer.close()
