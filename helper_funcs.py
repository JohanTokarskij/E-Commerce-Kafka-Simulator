import os
import time


## HELPER FUNCTIONS #
def clear_screen(sleep_value=2):
        time.sleep(sleep_value)
        os.system('cls' if os.name == 'nt' else 'clear')

def display_time(shutdown_event):
    while not shutdown_event.is_set():
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print(f'Current Time: {current_time}')
        time.sleep(1)

def manage_output(shutdown_event, consumer_outputs):
     while not shutdown_event.is_set():
        clear_screen(2)
        for key, value in consumer_outputs.items():
            print(key, value)
