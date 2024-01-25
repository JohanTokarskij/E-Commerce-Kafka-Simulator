from kafka import KafkaProducer
import json
from faker import Faker
from time import sleep

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode())
faker = Faker()


for x in range(101):
    producer.send('test_topic', {'message': f"{faker.credit_card_provider()} - {faker.credit_card_number()}"})
    producer.flush()
    x+=1
    sleep(0.25)