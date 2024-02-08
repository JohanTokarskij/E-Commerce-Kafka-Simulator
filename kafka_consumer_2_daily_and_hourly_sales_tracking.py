from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import deque
from kafka_utility_functions import load_state, save_state, total_daily_and_hourly_sales


def daily_and_hourly_sales_tracking_consumer(shutdown_event, consumer_output):
    consumer_2 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='sales_tracking_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    state_file = 'kafka_consumer_2_daily_and_hourly_sales_state.json'
    default_state = {
        'current_date': datetime.now().date().isoformat(),
        'daily_sales': 0,
        'hourly_sales_data': []
    }
    
    total_daily_and_hourly_sales(consumer_2, state_file, default_state, shutdown_event, consumer_output)


# DEBUG:
""" import threading
shutdown_event = threading.Event()
consumer_output = {}
daily_and_hourly_sales_tracking_consumer(shutdown_event, consumer_output) """