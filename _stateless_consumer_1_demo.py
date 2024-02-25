import json
from datetime import datetime
from kafka import KafkaConsumer
from helper_funcs import clear_screen


def daily_order_count_consumer():
    consumer_1 = KafkaConsumer(
        'stateless-demo',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode()))
    
    order_count = 0
    current_date = datetime.now().date()

    try:
        while True:
            messages = consumer_1.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        order_date = datetime.strptime(
                            message.value['ordertime'], '%Y-%m-%d %H:%M:%S').date()

                        if order_date > current_date:
                            order_count = 0

                        order_count += 1
                    clear_screen(1)
                    print(f'Orders since midnight: {order_count}')
    except Exception as e:
        print(f'Error processing messages: {e}')
    finally:
        consumer_1.close()

daily_order_count_consumer()
# group_id='stateless-demo', auto_offset_reset='latest',