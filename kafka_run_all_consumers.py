import threading
import time
from pprint import pprint
from store_initialization import products
from kafka_consumer_1_daily_order_count import daily_order_count_consumer
from kafka_consumer_2_daily_and_hourly_sales_tracking import daily_and_hourly_sales_tracking_consumer
from kafka_consumer_3_daily_sales_report import daily_sales_report_consumer
from kafka_consumer_4_inventory_updates import inventory_management_consumer
from helper_funcs import clear_screen


shutdown_event = threading.Event()

def manage_output(shutdown_event, consumer_output):
    while not shutdown_event.is_set():
        clear_screen(1)
        for key, value in consumer_output.items():
            print(key)
            if isinstance(value, dict) and key == 'Inventory Update: ':
                print("{:<3} {:<30} {:>10}".format("ID", "Product Name", "Quantity"))
                for pid, details in value.items():
                    print("{:<3} {:<30} {:>10}".format(pid, details['name'], details['quantity']))
            else:
                pprint(value)
            print('\n')


consumer_output = {
    'Orders since midnight: ': 'Waiting for update...',
    'Total sales for today: ': 'Waiting for update...',
    'Total sales for the past hour: ': 'Waiting for update...',
    'Time left until next daily report: ': 'Waiting for update...',
    'Inventory Update: ': 'Waiting for update...'
}

consumers = [
    (daily_order_count_consumer, (shutdown_event, consumer_output)),
    (daily_and_hourly_sales_tracking_consumer, (shutdown_event, consumer_output)),
    (daily_sales_report_consumer, (shutdown_event, consumer_output)),
    (inventory_management_consumer, (shutdown_event, consumer_output, products))
]

consumer_threads = []

if __name__ == '__main__':
    for consumer, args in consumers:
        thread = threading.Thread(target=consumer, args=args)
        thread.start()
        consumer_threads.append(thread)
    
    output_manager_thread = threading.Thread(target=manage_output, args=(shutdown_event, consumer_output))
    output_manager_thread.start()

    try:
        while not shutdown_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print('Stopping consumers...')
        shutdown_event.set()
    for thread in consumer_threads + [output_manager_thread]:
        thread.join()

    
