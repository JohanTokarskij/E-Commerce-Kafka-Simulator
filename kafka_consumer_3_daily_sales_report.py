import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka_utility_functions import load_state, save_state, generate_and_save_report
from helper_funcs import calculate_time_until_midnight


def daily_sales_report_consumer(shutdown_event, consumer_output):
    """
    Generates daily sales reports from 'e-commerce-orders' Kafka topic at midnight. Outputs report timing to a shared dictionary "consumer_output" in real time.
    
    Keeps track of progress between restarts by maintaining state in a JSON file, including order count, daily sales, and product counts.
    
    Listens for a shutdown signal provided by "shutdown_event", a threading.Event, to terminate gracefully. 
    """
    
    consumer_3 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='daily_sales_report_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode()))

    state_file = './kafka_states/kafka_consumer_3_daily_report_state.json'
    default_state = {'current_date': datetime.now().date().isoformat(),
                     'order_count': 0,
                     'daily_sales': 0,
                     'product_counts': {}}

    state = load_state(state_file, default_state)

    current_date = datetime.fromisoformat(state['current_date']).date()
    order_count = state['order_count']
    daily_sales = state['daily_sales']
    product_counts = state['product_counts']

    try:
        while not shutdown_event.is_set():
            messages = consumer_3.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        order_date = datetime.strptime(
                            message.value['ordertime'], '%Y-%m-%d %H:%M:%S').date()
                        if order_date > current_date:
                            current_date = order_date
                            order_count = 0
                            daily_sales = 0
                            product_counts = {}

                        order_count += 1
                        order_amount = round(sum(
                            order_detail['quantity'] * order_detail['product']['price'] for order_detail in message.value['order_details']), 2)
                        daily_sales += order_amount

                        for item in message.value['order_details']:
                            product_name = item['product']['name']
                            quantity = item['quantity']
                            price = round(item['product']['price'], 2)
                            if product_name not in product_counts:
                                product_counts[product_name] = {
                                    'quantity': 0, 'total': 0}
                            product_counts[product_name]['quantity'] += quantity
                            product_counts[product_name]['total'] += round(
                                (price * quantity), 2)

                        state = {'current_date': current_date.isoformat(),
                                 'order_count': order_count,
                                 'daily_sales': round(daily_sales, 2),
                                 'product_counts': product_counts}
                        save_state(state_file, state)

                        now = datetime.now()
                        if now.date() > current_date:
                            report_date = datetime.strptime(
                                state['current_date'], '%Y-%m-%d')
                            generate_and_save_report(state, report_date.date())

                            current_date = now.date()
                            state['current_date'] = current_date.isoformat()
                            state['order_count'] = 0
                            state['daily_sales'] = 0
                            state['product_counts'] = {}

                            save_state(state_file, state)
                        time_left = calculate_time_until_midnight()
                        hours, remainder = divmod(time_left.seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        consumer_output[
                            'Time left until next daily report:'] = f"{hours} hours, {minutes} minutes, {seconds} seconds left"

    except Exception as e:
        print(f'Error processing messages: {e}')
    finally:
        consumer_3.close()
