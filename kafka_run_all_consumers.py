import threading
import signal
import time
from kafka_consumer_1_total_daily_order_count import daily_order_count_consumer
from kafka_consumer_2_total_daily_and_hourly_sales import sales_tracking_consumer
from helper_funcs import manage_output, display_time

shutdown_event = threading.Event()

consumer_outputs = {
    'Current Time: ': 'Waiting for update...',
    'Orders since midnight: ': 'Waiting for update...',
    'Total sales for today: ': 'Waiting for update...',
    'Total sales for the past hour: ': 'Waiting for update...',
}

consumers = [daily_order_count_consumer
             ]

consumer_threads = []

def signal_handler(sig, frame):
    shutdown_event.set()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    for consumer in consumers:
        thread = threading.Thread(target=consumer, args=(shutdown_event, consumer_outputs))
        thread.start()
        consumer_threads.append(thread)
    
    output_manager_thread = threading.Thread(target=manage_output, args=(shutdown_event, consumer_outputs))
    output_manager_thread.start()

    try:
        while not shutdown_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print('Stopping consumers...')
        shutdown_event.set()
    for thread in consumer_threads + [output_manager_thread]:
        thread.join()

    
