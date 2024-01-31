from kafka import KafkaConsumer
import json
from pprint import pprint

consumer = KafkaConsumer('e-commerce-orders', 
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='latest',
                         value_deserializer=lambda m: json.loads(m.decode()))

while True:
    for message in consumer:
        pprint(f'{message.value}')
        print('\n')
    