





from confluent_kafka import Producer



producer_config = {
    'bootstrap.servers': 'b-2-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196,b-1-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196',  # Replace with your Kafka broker address
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',  # Use SCRAM-SHA-256 or the mechanism you chose.
    'sasl.username': 'KAFKA_CONNECT',  # Replace with the Kafka username
    'sasl.password': 'client01',  # Replace with the Kafka password
    'enable.ssl.certificate.verification':'false',
    'ssl.ca.location': './certificates/main.crt',
}



producer = Producer(producer_config)

# Produce a message to the topic
message_key = ""
message_value = "Hello, Kafka!"

producer.produce(topic='test-topic', key=message_key, value=message_value)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()

# Close the producer when done
# producer.close()

print("Message sent successfully.")