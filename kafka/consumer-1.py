from confluent_kafka import Consumer,KafkaError


consumer_config_1 = {
    'bootstrap.servers': 'b-2-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196,b-1-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196',  # Replace with your Kafka broker address
    'group.id': '',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',  # Use SCRAM-SHA-256 or the mechanism you chose.
    'sasl.username': 'KAFKA_CONNECT',  # Replace with the Kafka username
    'sasl.password': 'client01',  # Replace with the Kafka password
    'enable.ssl.certificate.verification':'false',
    'ssl.ca.location': './certificates/main.crt',
}


consumer_1 = Consumer(consumer_config_1)


consumer_1.subscribe(['test-topic'])


while True:
    msg = consumer_1.poll(5.0)  # Poll for new messages with a timeout of 1 second
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f"Consumer 1: Reached end of partition: {msg.partition()}")
        else:
            print(f"Consumer 1: Error while consuming message: {msg.error()}")
    else:
        print(f"Consumer 1: Received message: value={msg.value()}")

