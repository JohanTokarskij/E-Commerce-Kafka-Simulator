import os
import time
from datetime import datetime, timedelta

## HELPER FUNCTIONS #
def clear_screen(sleep_value=2):
        time.sleep(sleep_value)
        os.system('cls' if os.name == 'nt' else 'clear')

def setup_environment():
    """
    Set up the necessary environment for the application.
    """

    if not os.path.exists('./reports'):
        os.makedirs('./reports')

    if not os.path.exists('./kafka_states'):
        os.makedirs('./kafka_states')

def calculate_time_until_midnight():
    now = datetime.now()
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    time_left = midnight - now
    return time_left