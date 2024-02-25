import json
from datetime import datetime
from time import sleep
import random
from kafka import KafkaProducer
from store_initialization import products, order_id, CUSTOMER_ID

def generate_order():
    global order_id
    order = {
        'order_id': order_id,
        'order_details': [
            {
                'product': random.choice(products),
                'quantity': random.randint(1,5)
            } for _ in range(random.randint(1,5))
        ],
        'customer_id': random.choice(CUSTOMER_ID),
        'ordertime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    }
    order_id += 1
    return order

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode())

while True:
    order = generate_order()
    producer.send('stateless-demo', order)
    producer.flush()
    sleep(random.uniform(0.1, 0.5))