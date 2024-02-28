import json
from time import sleep
import random
from kafka import KafkaProducer
from store_initialization import generate_order
from helper_funcs import clear_screen


def order_producer():
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                value_serializer=lambda v: json.dumps(v).encode())
        
        print('Order producer has been started.')
        while True:
            order = generate_order()
            producer.send('e-commerce-orders', order)
            producer.flush()
            sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print('Shutting down order producer.')
        clear_screen()

if __name__ == '__main__':
    order_producer()