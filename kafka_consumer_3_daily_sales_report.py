from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from kafka_utility_functions import load_state, save_state

def daily_sales_report_consumer(shutdown_event, consumer_outputs):
    consumer_3 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='daily_sales_report_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )