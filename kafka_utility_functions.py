import json
from datetime import datetime, timedelta
from collections import deque


# STATE MANAGEMENT FOR CONSUMERS:
def save_state(file_path, state):
    with open(file_path, 'w') as file:
        json.dump(state, file)

def load_state(file_path, default_state):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return default_state

def generate_and_save_report(state, report_date):
    filename = f'{report_date.strftime("%Y-%m-%d")}_sales_report.txt'
    with open(filename, 'w') as file:
        file.write(f'Daily Sales Report for {report_date}\n\n')
        file.write(f'Total Orders: {state["order_count"]}\n\n')
        file.write(f'Total Sales: ${state["daily_sales"]:.2f}\n\n')
        file.write('Sales per Product:\n')
        for product_name, details in state["product_counts"].items():
            file.write(f'{product_name}: {details["quantity"]} sold, Total: ${details["total"]:.2f}\n')
    print(f'Report saved to {filename}')

    

# Utility function for consumer_1 and consumer_3:
""" def total_daily_order_count(consumer, state_file, default_state, shutdown_event, consumer_output):
    state = load_state(state_file, default_state)
    order_count = state.get('order_count', 0)
    current_date = datetime.fromisoformat(state.get('current_date')).date()

    try:
        while not shutdown_event.is_set():
            messages = consumer.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        order_date = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S').date()

                        if order_date > current_date:
                            current_date = order_date
                            order_count = 0
                        
                        order_count +=1

                        state = {'current_date': current_date.isoformat(), 
                                'order_count': order_count}
                        save_state(state_file, state)
                    consumer_output['Orders since midnight: '] = order_count

    except Exception as e:
        print(f'Error processing messages: {e}')
    finally:
        consumer.close() """


# Utility function for consumer_2 and consumer_3:
""" def total_daily_and_hourly_sales(consumer, state_file, default_state, shutdown_event, consumer_output):
    state = load_state(state_file, default_state)
    daily_sales = state.get('daily_sales', 0)
    hourly_sales_data = deque([(datetime.fromisoformat(timestamp), amount) for timestamp, amount in state.get('hourly_sales_data', [])])
    current_date = datetime.fromisoformat(state.get('current_date')).date()

    try:
        while not shutdown_event.is_set():
            messages = consumer.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        order_time = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S')
                        order_date = order_time.date()
                        order_amount = round(sum(order_detail['quantity'] * order_detail['product']['price'] for order_detail in message.value['order_details']), 2)

                        if order_date > current_date:
                            current_date = order_date
                            daily_sales = 0
                        
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
                    consumer_output['Total sales for today: '] = round(daily_sales, 2)
                    consumer_output['Total sales for the past hour: '] = round(hourly_sales_total, 2)

    except Exception as e:
        print(f'Error processing messages: {e}')
    finally:
        consumer.close() """
