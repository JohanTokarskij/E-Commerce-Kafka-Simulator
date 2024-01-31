from datetime import datetime
from time import sleep
import random
import json
from kafka import KafkaProducer
from faker import Faker
from faker_commerce import Provider



fake = Faker()
fake.add_provider(Provider)


order_id = 1
CUSTOMER_ID = [id for id in range(1, 1000)]
products = [{'product_id': i, 'name': fake.ecommerce_name(), 'price': round(random.uniform(10, 500), 2)} for i in range(1, 11)]

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode())

def generate_order():
    global order_id
    order = {
        'order_id': order_id,
        'order_details': [(random.choice(products), random. randint(1, 10)) for _ in range(random.randint(1, 5))],
        'customer_id': random.choice(CUSTOMER_ID),
        'ordertime': datetime.now().strftime('%Y-%m-%d-%H:%M:%S')

    }
    order_id += 1
    return order

for _ in range(101):
    order = generate_order()
    producer.send('e-commerce-orders', order)
    producer.flush()
    sleep(1)