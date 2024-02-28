import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from opensearchpy import OpenSearch
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


def manage_console_output(shutdown_event, consumer_output):
    """
    Manages consumer output in the console, updating every 1 second until shutdown_event is set.

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
            if isinstance(value, dict):
                if key == 'Inventory Update:':
                    print(f'{"ID":>8} {"Product Name":<25} {"Quantity":^8}')
                    for pid, details in value.items():
                        print(f'{pid:>8} {details["name"]:.<25} {details["quantity"]:<5}')
                    print('\n')
                    print(f'Inventory Auto-Refill Threshold: \n\tItems falling below {product_refill_threshold} will be refilled.')
                    print(f'Inventory Auto-Refill Amount: \n\tEach refill operation adds {product_refill_amount} items.')         
                elif key == 'Processed orders statistics:':
                    print(f'{" ":>5} {"Time Window:":<28} {"Order Count:":^5}')
                    for time_window, order_count in value.items():
                        print(f'{" ":>5} {time_window:.<28} {order_count:<5}')
            else:
                print(f'{value:^47}')
        print('+', '-'*45, '+')


def manage_opensearch_output(shutdown_event, consumer_output):
    """
    Sends consumer output data to OpenSearch at 1-second intervals until shutdown_event is set. 
    It formats the consumer data for OpenSearch and handles its transmission to a specified index. 

    Args:
        shutdown_event (threading.Event): Signals when to stop the function.
        consumer_output (dict): Output from consumers to be sent to OpenSearch.

    Utilizes environment variables for OpenSearch credentials and endpoint.

    Returns:
        None
    """
    load_dotenv()
    OPENSEARCH_USERNAME = os.getenv('OPENSEARCH_USERNAME')
    OPENSEARCH_PASSWORD = os.getenv('OPENSEARCH_PASSWORD')
    OPENSEARCH_ENDPOINT = os.getenv('OPENSEARCH_ENDPOINT')

    os_client= OpenSearch(
        hosts=[{'host': OPENSEARCH_ENDPOINT, 'port': 443}],
        http_compress=True,
        http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
        use_ssl=True,
        verify_certs=True,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    index_name = 'consumer_output'
    
    while not shutdown_event.is_set():
        for key, value in consumer_output.items():
            if isinstance(value, dict):
                if key == 'Inventory Update:':
                    for sub_key, sub_value in value.items():
                        document = {
                            'metric': f'{sub_value["name"]} (ProductID {sub_key})',
                            'value': str(sub_value['quantity']), 
                            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                        }
                        try:
                            os_client.index(index=index_name, body=document)
                        except Exception as e:
                            print(f"Error indexing document: {e}")
                elif key == 'Processed orders statistics:':
                    for sub_key, sub_value in value.items():
                        document = {
                            'metric': str(sub_key),
                            'value': str(sub_value),
                            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                        }
                        try:
                            os_client.index(index=index_name, body=document)
                        except Exception as e:
                            print(f"Error indexing document: {e}")
            else:
                value_str = str(value)
                document = {
                    'metric': key,
                    'value': value_str,
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                }
                try:
                    os_client.index(index=index_name, body=document)
                except Exception as e:
                    print(f'Error indexing document: {e}')
        time.sleep(1)



def generate_and_save_report(state, report_date):
    """
    Generate and save a daily sales report for a specified date.

    Args:
        state (dict): The state containing sales data.
        report_date (datetime.date): The date for which the report is generated.

    Returns:
        None
    """
    
    filename = f'./reports/{report_date.strftime("%Y-%m-%d")}_sales_report.txt'
    with open(filename, 'w') as file:
        file.write(f'Daily Sales Report for {report_date}\n\n')
        file.write(f'Total Orders: {state["order_count"]}\n\n')
        file.write(f'Total Sales: {state["daily_sales"]:.2f} $\n\n')
        file.write('Sales per Product:\n')
        for product_name, details in state["product_counts"].items():
            file.write(f'{product_name}: {details["quantity"]} sold, Total: {details["total"]:.2f} $\n')
    print(f'Report saved to {filename}')


def send_email_simulation(order_id, customer_id):
    with open('./reports/order_emails.txt', 'a') as file:
        file.write(f'Order {order_id} for Customer {customer_id} has been handled and dispatched at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}.\n')


def update_time_windows(time_windows, current_time):
    for key, window in time_windows.items():
        if key == '1. Last 5 minutes':
            time_limit = current_time - timedelta(minutes=5)
        elif key == '2. Last 30 minutes':
            time_limit = current_time - timedelta(minutes=30)
        elif key == '3. Last hour':
            time_limit = current_time - timedelta(hours=1)
        elif key == '4. Last 2 hours':
            time_limit = current_time - timedelta(hours=2)

        while window and window[0] < time_limit:
            window.popleft()