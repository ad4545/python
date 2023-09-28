from confluent_kafka import Producer
from fastapi import FastAPI, Response
from data_model import VDAMessage
import json
from pydantic import BaseModel
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest


app = FastAPI()


REQUEST_NUMBER = Counter('request_process_seconds', 'count')

class Sample(BaseModel):
    id:int
    name:str


# Define a Kafka producer

producer_config = {
    'bootstrap.servers': '[b-1.kafkacluster.vwpk16.c3.kafka.ap-south-1.amazonaws.com:9092,b-2.kafkacluster.vwpk16.c3.kafka.ap-south-1.amazonaws.com:9092]',
    'client.id': 'python-producer'
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


@app.get('/metrics')
def metrics():
    return Response(generate_latest(), content_type=CONTENT_TYPE_LATEST)
