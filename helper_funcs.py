import os
import time


## HELPER FUNCTIONS #
def clear_screen(sleep_value=2):
        time.sleep(sleep_value)
        os.system('cls' if os.name == 'nt' else 'clear')

def display_time(shutdown_event, consumer_outputs):
    while not shutdown_event.is_set():
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        consumer_outputs['Current Time: ']  = current_time
        time.sleep(1)
