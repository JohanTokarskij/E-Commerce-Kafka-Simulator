from kafka import KafkaConsumer
import json
from datetime import datetime
from state_management import load_state, save_state


def daily_order_count_consumer(shutdown_event, consumer_outputs):
    consumer_1 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='daily_order_count_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    state_file = 'kafka_consumer_1_daily_order_count_state.json'
    default_state = {'order_count': 0, 'current_date': datetime.now().date().isoformat()}
    
    state = load_state(state_file, default_state)
    order_count = state.get('order_count', 0)
    current_date = datetime.fromisoformat(state.get('current_date')).date()

    try:
        while not shutdown_event.is_set():
            messages = consumer_1.poll(timeout_ms=1000)
            for msgs in messages.values():
                for message in msgs:
                    order_date = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S').date()

                    if order_date > current_date:
                        current_date = order_date
                        order_count = 0
                    
                    order_count += 1

                    state = {"order_count": order_count, "current_date": current_date.isoformat()}
                    save_state(state_file, state)
                    consumer_outputs['Orders since midnight: '] = order_count
    except Exception as e:
        print(f"Error processing messages: {e}")
    finally:
        consumer_1.close()
        print("Consumer_1 closed.")


# DEBUG:
""" import threading
shutdown_event = threading.Event()
consumer_outputs = {}
daily_order_count_consumer(shutdown_event, consumer_outputs) """