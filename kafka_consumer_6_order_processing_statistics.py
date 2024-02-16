import json
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer
from kafka_utility_functions import load_state, save_state
from kafka_utility_functions import update_time_windows


def order_processing_statistics_consumer(shutdown_event, consumer_output):
    """
    Collects and summarizes processing statistics of orders from the 'processed-orders' Kafka topic. Outputs statistics to a shared dictionary "consumer_output" in real time.
    
    Maintains a rolling count of processed orders across multiple time windows (last 5 minutes, 30 minutes, hour, 2 hours, avg 5 mins/2 hours). 
    
    Keeps track of progress between restarts by maintaining state in a JSON file. 
    
    Listens continuously until receiving a shutdown signal via "shutdown_event", a threading.Event, ensuring graceful termination.
    """
     
    consumer_6 = KafkaConsumer(
        'processed-orders',
        bootstrap_servers='localhost:9092',
        group_id='processing_statistics_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    state_file = './kafka_states/kafka_consumer_6_order_processing_state.json'
    default_state = {'1. Last 5 minutes': [],
                     '2. Last 30 minutes': [],
                     '3. Last hour': [],
                     '4. Last 2 hours': []}
    state = load_state(state_file, default_state)

    # Convert lists of state to deque:
    time_windows = {key: deque([datetime.fromisoformat(timestamp) for timestamp in state[key]])
                    for key in state}
        
    try:
        while not shutdown_event.is_set():
            current_time = datetime.now()
            messages = consumer_6.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        order_time = datetime.strptime(message.value['ordertime'], '%Y-%m-%d %H:%M:%S')

                        for window in time_windows:
                            time_windows[window].append(order_time) 
                        
                        update_time_windows(time_windows, current_time)

                        state = {key: list(datetime.isoformat(timestamp) for timestamp in time_windows[key]) for key in time_windows}

                        save_state(state_file, state)

                        if '4. Last 2 hours' in time_windows:
                            total_orders_last_2_hours = len(time_windows['4. Last 2 hours'])
                            average_orders_per_5_min_last_2_hours = total_orders_last_2_hours / 24
                        else:
                            average_orders_per_5_min_last_2_hours = 0

                        consumer_output['Processed orders statistics:'] = {
                            key: len(state[key]) for key in state.keys()
                        }
                        consumer_output['Processed orders statistics:']['5. Avg 5 mins/last 2 hours'] = int(average_orders_per_5_min_last_2_hours)

    except Exception as e:
        print(f'Error in handled orders monitoring: {e}')
    finally:
        consumer_6.close()