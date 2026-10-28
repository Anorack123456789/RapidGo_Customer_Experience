import json
import time
import random
import uuid
from kafka import KafkaProducer

# Configuración del productor (Ajusta 'localhost:9092' si tu Docker usa otro puerto)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)