from kafka import KafkaConsumer
import json
from datetime import datetime
from kafka_utility_functions import load_state, save_state, total_daily_order_count


def daily_order_count_consumer(shutdown_event, consumer_output):
    consumer_1 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='daily_order_count_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    state_file = 'kafka_consumer_1_daily_order_count_state.json'
    default_state = {'order_count': 0, 'current_date': datetime.now().date().isoformat()}

    total_daily_order_count(consumer_1, state_file, default_state, shutdown_event, consumer_output)
    


# DEBUG:
""" import threading
shutdown_event = threading.Event()
consumer_output = {}
daily_order_count_consumer(shutdown_event, consumer_output) """
