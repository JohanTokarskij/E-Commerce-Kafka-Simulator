from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import deque


def sales_tracking_consumer(shutdown_event, consumer_outputs):
    consumer_2 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        enable_auto_commit=False,
        group_id='sales_tracking_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    daily_sales = 0
    hourly_sales = deque()

    current_date = datetime.now().date()
    try:
        while not shutdown_event.is_set():
            messages = consumer_2.poll(timeout_ms=1000)
            for msgs in messages.values():
                for message in msgs:
                    
                    order_date = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S').date()
                    order_amount = round(sum(order_detail['quantity'] * order_detail['product']['price'] for order_detail in message.value['order_details']), 2)

                    if order_date > current_date:
                        current_date = order_date
                        daily_sales = 0
                    
                    daily_sales += order_amount

                    # IMPLEMENT HOURLY SALES!

            consumer_outputs['Total sales for today: '] = round(daily_sales, 2)
            consumer_2.commit()
    except KeyboardInterrupt:
        consumer_2.commit()
    finally:
        consumer_2.commit()
        consumer_2.close()
        print("Consumer closed.")

""" import threading
shutdown_event = threading.Event()
consumer_outputs = {}
sales_tracking_consumer(shutdown_event, consumer_outputs) """