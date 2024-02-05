from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import deque

import threading
shutdown_event = threading.Event()

def sales_tracking_consumer(shutdown_event, consumer_outputs):
    consumer_2 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    daily_sales = 0
    hourly_sales = deque()

    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    one_hour_ago = datetime.now() - timedelta(hours=1)

    while not shutdown_event.is_set():
        messages = consumer_2.poll(timeout_ms=1000)
        for msgs in messages.values():
            for message in msgs:
                order_time = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S')
                order_amount = round(sum(order_detail['quantity'] * order_detail['product']['price'] for order_detail in message.value['order_details']), 2)

                if order_time >= midnight + timedelta(days=1):
                    midnight += timedelta(days=1)
                    daily_sales=0
                
                if order_time >= midnight:
                    daily_sales += order_amount

                # IMPLEMENT HOURLY SALES!

        consumer_outputs['Total sales for today: '] = round(daily_sales, 2)

