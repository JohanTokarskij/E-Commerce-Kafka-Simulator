import random
from faker import Faker
from faker_commerce import Provider

fake = Faker()
fake.add_provider(Provider)

order_id = 1
CUSTOMER_ID = [id for id in range(1, 1000)]
products = [{'product_id': i, 
             'name': fake.ecommerce_name(), 
             'price': round(random.uniform(10, 50), 2)} for i in range(1, 11)]