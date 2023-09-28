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
    'bootstrap.servers': 'b-1.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9092,b-2.kafkamskcluster.e0b8oe.c2.kafka.ap-south-1.amazonaws.com:9092',
    'client.id': 'python-producer',
    'acks':'all',
    'compression.type':'snappy',
    'batch.num.messages':1000,
    'linger.ms':10
}
producer = Producer(producer_config)


@app.get('/')
def status():
    return {"message": "Hello from server"}


@app.post('/kafka')
def send_message(msg: dict):
    # REQUEST_NUMBER.inc()

    # Produce a message
    topic = 'stream-topic'
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

