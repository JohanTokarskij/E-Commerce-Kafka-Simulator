import json
from datetime import datetime, timedelta
from collections import deque
from kafka import KafkaConsumer

def order_processing_statistics_consumer(shutdown_event, consumer_output):
    consumer_6 = KafkaConsumer(
        'processed-orders',
        bootstrap_servers='localhost:9092',
        group_id='processing_statistics_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )