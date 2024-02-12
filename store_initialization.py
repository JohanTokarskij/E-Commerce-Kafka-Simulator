import random
from faker import Faker
from faker_commerce import Provider

fake = Faker()
fake.add_provider(Provider)

order_id = 1
CUSTOMER_ID = [id for id in range(1, 1000)]
products = [{'product_id': str(i), 
             'name': fake.ecommerce_name().split(' ')[-1], 
             'price': round(random.uniform(10, 50), 2)} for i in range(1, 11)]

product_refill_threshold = 30
product_refill_amount = 100
