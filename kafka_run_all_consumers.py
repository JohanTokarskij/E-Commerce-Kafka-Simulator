import time
import threading
from store_initialization import products, product_refill_amount, product_refill_threshold
from kafka_utility_functions import manage_console_output, manage_opensearch_output
from kafka_consumer_1_daily_order_count import daily_order_count_consumer
from kafka_consumer_2_daily_and_hourly_sales_tracking import daily_and_hourly_sales_tracking_consumer
from kafka_consumer_3_daily_sales_report import daily_sales_report_consumer
from kafka_consumer_4_inventory_management import inventory_management_consumer
from kafka_consumer_5_and_order_processing_producer import order_processing_consumer
from kafka_consumer_6_order_processing_statistics import order_processing_statistics_consumer
from helper_funcs import setup_environment


shutdown_event = threading.Event()

# Shared dictionary for consumer outputs:
consumer_output = {
    'Orders since midnight:': 'Waiting for update...',
    'Total sales for today:': 'Waiting for update...',
    'Total sales for the past hour:': 'Waiting for update...',
    'Time left until next daily report:': 'Waiting for update...',
    'Inventory Update:': 'Waiting for update...',
    'Processed orders statistics:': 'Waiting for update...'
}

consumers = [
    (daily_order_count_consumer, (shutdown_event, consumer_output)),
    (daily_and_hourly_sales_tracking_consumer, (shutdown_event, consumer_output)),
    (daily_sales_report_consumer, (shutdown_event, consumer_output)),
    (inventory_management_consumer, (shutdown_event, consumer_output, products, product_refill_amount, product_refill_threshold)),
    (order_processing_consumer, (shutdown_event, )),
    (order_processing_statistics_consumer, (shutdown_event, consumer_output))
]

consumer_threads = []

if __name__ == '__main__':
    setup_environment()

    for consumer, args in consumers:
        thread = threading.Thread(target=consumer, args=args)
        thread.start()
        consumer_threads.append(thread)
    
    output_manager_thread = threading.Thread(target=manage_console_output, args=(shutdown_event, consumer_output))
    output_manager_thread.start()

    opensearch_manager_thread = threading.Thread(target=manage_opensearch_output, args=(shutdown_event, consumer_output))
    opensearch_manager_thread.start()

    try:
        while not shutdown_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print('Stopping consumers and OpenSearch sender...')
        shutdown_event.set()
    for thread in consumer_threads + [output_manager_thread, opensearch_manager_thread]:
        thread.join()