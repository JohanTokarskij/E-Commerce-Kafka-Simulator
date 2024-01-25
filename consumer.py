from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('test_topic', 
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode()))

while True:
    for message in consumer:
        print(f'{message.value.get("message")}')
    