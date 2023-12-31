from confluent_kafka import Consumer,KafkaError


consumer_config_2 = {
    'bootstrap.servers': 'b-1.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9092,b-2.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9092',  # Replace with your Kafka broker address
    'group.id': 'group-1',
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}


consumer_2 = Consumer(consumer_config_2)


consumer_2.subscribe(['test-topic'])


while True:
    msg = consumer_2.poll(1.0)  # Poll for new messages with a timeout of 1 second
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f"Consumer 1: Reached end of partition: {msg.partition()}")
        else:
            print(f"Consumer 1: Error while consuming message: {msg.error()}")
    else:
        print(f"Consumer 1: Received message: value={msg.value()}")

