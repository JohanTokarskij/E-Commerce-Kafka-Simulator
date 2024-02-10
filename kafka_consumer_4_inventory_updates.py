from kafka import KafkaConsumer
import json
from datetime import datetime
from time import sleep
from kafka_utility_functions import load_state, save_state
from pprint import pprint

def inventory_management_consumer(shutdown_event, consumer_output, products):
    consumer_4 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='inventory_management_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode()))
    
    state_file  = 'kafka_consumer_4_inventory_state.json'
    default_inventory_state = {str(product['product_id']): {'name': product['name'], 'quantity': 100} for product in products}    
    inventory = load_state(state_file, default_inventory_state)

    refill_threshold = 30
    refill_amount = 100
    try:
        while not shutdown_event.is_set():
            messages = consumer_4.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        for item in message.value['order_details']:
                            product_id = str(item['product']['product_id'])
                            quantity = item['quantity']
                            if product_id in inventory:
                                inventory[product_id]['quantity'] -= quantity

                                if inventory[product_id]['quantity'] < refill_threshold:
                                    print(f'Refilling {product_id} - {inventory[product_id]["name"]}')
                                    inventory[product_id]['quantity'] += refill_amount
                            else:
                                print(f"Product ID {product_id} not found in inventory")

                            save_state(state_file, inventory)

                            consumer_output['Inventory Update: '] = {pid: inventory[pid] for pid in sorted(inventory.keys(), key=int)}            
    except Exception as e:
        print(f'Error processing messages: {e}')
    finally:
        consumer_4.close() 

""" # DEBUG:
import threading
from store_initialization import products
shutdown_event = threading.Event()
consumer_output = {}
inventory_management_consumer(shutdown_event, consumer_output, products) """