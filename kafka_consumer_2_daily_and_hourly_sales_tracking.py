import json
from datetime import datetime, timedelta
from collections import deque
from kafka import KafkaConsumer
from kafka_utility_functions import load_state, save_state


def daily_and_hourly_sales_tracking_consumer(shutdown_event, consumer_output):
    consumer_2 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='sales_tracking_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode()))

    state_file = './kafka_states/kafka_consumer_2_daily_and_hourly_sales_state.json'
    default_state = {'current_date': datetime.now().date().isoformat(),
                     'daily_sales': 0,
                     'hourly_sales_data': []}

    state = load_state(state_file, default_state)
    current_date = datetime.fromisoformat(state['current_date']).date()
    daily_sales = state['daily_sales']
    hourly_sales_data = deque([(datetime.fromisoformat(timestamp), amount) for timestamp, amount in state.get('hourly_sales_data', [])])

    try:
        while not shutdown_event.is_set():
            messages = consumer_2.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        order_time = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S')
                        order_date = order_time.date()

                        if order_date > current_date:
                            current_date = order_date
                            daily_sales = 0
                        
                        order_amount = round(sum(order_detail['quantity'] * order_detail['product']['price'] for order_detail in message.value['order_details']), 2)
                        daily_sales += order_amount

                        hourly_sales_data.append((order_time, order_amount))

                        one_hour_ago = datetime.now() - timedelta(hours=1)
                        while hourly_sales_data and hourly_sales_data[0][0] < one_hour_ago:
                            hourly_sales_data.popleft()
                        
                        hourly_sales_total = sum(amount for _, amount in hourly_sales_data if _ >= one_hour_ago)

                        state = {
                            'current_date': current_date.isoformat(),
                            'daily_sales': round(daily_sales, 2),
                            'hourly_sales_data': [(timestamp.isoformat(), amount) for timestamp, amount in hourly_sales_data]
                        }
                        save_state(state_file, state)
                        consumer_output['Total sales for today: '] = f'{round(daily_sales, 2)} $'
                        consumer_output['Total sales for the past hour: '] = f'{round(hourly_sales_total, 2)} $'

    except Exception as e:
        print(f'Error processing messages: {e}')
    finally:
        consumer_2.close()


# DEBUG:
""" import threading
shutdown_event = threading.Event()
consumer_output = {}
daily_and_hourly_sales_tracking_consumer(shutdown_event, consumer_output) """
