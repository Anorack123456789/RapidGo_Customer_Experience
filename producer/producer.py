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

TOPIC = 'rapidgo.customer_feedback'
MOTIVOS = ["demora", "daño", "perdida", "excelente", "normal", "fraude"]

def generar_feedback():
    return {
        "order_id": str(uuid.uuid4()),
        "rating": random.randint(1, 5),
        "motivo": random.choice(MOTIVOS),
        "timestamp_ms": int(time.time() * 1000)
    }