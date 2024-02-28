import random
from datetime import datetime
from faker import Faker
from faker_commerce import Provider

fake = Faker()
fake.add_provider(Provider)

def generate_unique_product_names(n):
    unique_product_names = set()
    while len(unique_product_names) < n:
        product_name = fake.ecommerce_name().split(' ')[-1]
        unique_product_names.add(product_name)
    return list(unique_product_names)

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

order_id = 1
CUSTOMER_ID = [id for id in range(1, 1000)]
unique_names = generate_unique_product_names(10)

products = [{'product_id': str(i + 1), 
             'name': unique_names[i], 
             'price': round(random.uniform(10, 50), 2)} for i in range(10)]

product_refill_threshold = 50
product_refill_amount = 100