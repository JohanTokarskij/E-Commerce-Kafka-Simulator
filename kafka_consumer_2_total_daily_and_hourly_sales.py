from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from time import sleep
from helper_funcs import clear_screen

# TEST
def daily_order_count_consumer2(shutdown_event):
    consumer_1 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    order_count = 0
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    while not shutdown_event.is_set():
        print(f'Orders since midnight: {order_count}')
        messages = consumer_1.poll(timeout_ms=1000)
        for msgs in messages.values():
            for message in msgs:
                order_time = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S')
                if order_time >= midnight:
                    order_count +=1

        if datetime.now() >= midnight + timedelta(days=1):
            midnight += timedelta(days=1)
            order_count = 0
        sleep(1)
    
    consumer_1.close()