import threading
import time
from kafka_consumer_1_total_daily_order_count import daily_order_count_consumer
from kafka_consumer_2_total_daily_and_hourly_sales import daily_order_count_consumer2
from helper_funcs import display_time, clear_screen

shutdown_event = threading.Event()

consumer_outputs = {
    'Orders since midnight: ': 'Waiting for update...',
    'Total sales for the past day: ': 'Waiting for update...',
    'Total sales for the past hour: ': 'Waiting for update...',
}

def manage_output():
     while not shutdown_event.is_set():
        clear_screen(1)
        for key, value in consumer_outputs.items():
            print(key, value)

manage_output()

""" if __name__ == '__main__':
    consumers = [daily_order_count_consumer, daily_order_count_consumer2]
    threads = []

    for consumer in consumers:
        thread = threading.Thread(target=consumer, args=(shutdown_event,))
        thread.start()
        threads.append(thread)

    try:
        while True:
             clear_screen(0)
             time.sleep(1)
    except KeyboardInterrupt:
        print('Stopping consumers...')
        shutdown_event.set()

    for thread in threads:
            thread.join() """