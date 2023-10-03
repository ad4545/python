from confluent_kafka import Producer
from fastapi import FastAPI, Response
from data_model import VDAMessage
import json
from pydantic import BaseModel


app = FastAPI()



class Sample(BaseModel):
    id:int
    name:str


# Define a Kafka producer

producer_config = {
    'bootstrap.servers': 'b-2-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196,b-1-public.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9196',
    'client.id': 'python-producer',
    'acks':'all',
    'compression.type':'snappy',
    'batch.num.messages':1000,
    'linger.ms':10,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',  # Use SCRAM-SHA-256 or the mechanism you chose.
    'sasl.username': 'KAFKA_CONNECT',  # Replace with the Kafka username
    'sasl.password': 'client01',  # Replace with the Kafka password
}
producer = Producer(producer_config)


@app.get('/')
def status():
    return {"message": "Hello from server"}


@app.post('/kafka')
def send_message(msg: dict):
    # REQUEST_NUMBER.inc()

    # Produce a message
    topic = 'test-topic'
    # dictionary = msg.model_dump(mode='string')
    result = json.dumps(msg)
    producer.produce(topic, key='key', value=result)
    producer.flush()
    return {"message": 'Message sent'}


@app.post('/specific')
def send_specific(msg: VDAMessage):
    # Produce message
    topic = 'specific'
    # dictionary = msg.model_dump(mode='string')
    result = json.dumps(msg)
    producer.produce(topic, key='key', value=result)
    return {"message": "Message sent to a specific consumer"}

