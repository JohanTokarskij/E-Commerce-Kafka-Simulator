import json
import os
from datetime import datetime
from helper_funcs import clear_screen
from store_initialization import product_refill_threshold, product_refill_amount


def save_state(file_path, state):
    """
    Save the given state to a file specified by file_path.

    Args:
        file_path (str): The path to the file where the state will be saved.
        state (dict): The state to be saved as a dictionary.

    Returns:
        None
    """
    with open(file_path, 'w') as file:
        json.dump(state, file)


def load_state(file_path, default_state):
    """
    Load the state from a file specified by file_path. 
    If the file does not exist, return the default_state.

    Args:
        file_path (str): The path to the file from which the state will be loaded.
        default_state (dict): The default state to return if the file does not exist.

    Returns:
        dict: The loaded state or the default state if the file does not exist.
    """
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return default_state


def manage_output(shutdown_event, consumer_output):
    """
    Continuously manage the output for consumers in the terminal until the shutdown_event is set.

    Args:
        shutdown_event (threading.Event): An event to signal the function to stop running.
        consumer_output (dict): A dictionary containing the output of consumers to be displayed.

    Returns:
        None
    """
    if not os.path.exists('./kafka_states'):
        os.makedirs('./kafka_states')
        
    while not shutdown_event.is_set():
        clear_screen(1)
        for key, value in consumer_output.items():
            print('+', '-'*45, '+')
            print(f'{key:^47}')
            if isinstance(value, dict) and key == 'Inventory Update: ':
                print(f'{"ID":>8} {"Product Name":<25} {"Quantity":^10}')
                for pid, details in value.items():
                    print(f'{pid:>8} {details["name"]:.<25} {details["quantity"]:^5}')
            else:
                print(f'{value:^47}')
            print('+', '-'*45, '+', '\n')
        if any('Inventory Update: ' in key for key in consumer_output.keys()):
            print(f'Inventory Auto-Refill Threshold: Items falling below {product_refill_threshold} will be refilled')
            print(f'Inventory Auto-Refill Amount: Each refill operation adds {product_refill_amount} items')         


def generate_and_save_report(state, report_date):
    """
    Generate and save a daily sales report for a specified date.

    Args:
        state (dict): The state containing sales data.
        report_date (datetime.date): The date for which the report is generated.

    Returns:
        None
    """
    filename = f'{report_date.strftime("%Y-%m-%d")}_sales_report.txt'
    with open(filename, 'w') as file:
        file.write(f'Daily Sales Report for {report_date}\n\n')
        file.write(f'Total Orders: {state["order_count"]}\n\n')
        file.write(f'Total Sales: ${state["daily_sales"]:.2f}\n\n')
        file.write('Sales per Product:\n')
        for product_name, details in state["product_counts"].items():
            file.write(f'{product_name}: {details["quantity"]} sold, Total: ${details["total"]:.2f}\n')
    print(f'Report saved to {filename}')


def send_email_simulation(order_id, customer_id):
    with open('order_emails.txt', 'a') as file:
        file.write(f'Order {order_id} for Customer {customer_id} has been handled and dispatched at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}.\n')

