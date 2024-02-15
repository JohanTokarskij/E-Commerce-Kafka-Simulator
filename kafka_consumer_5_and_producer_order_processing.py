import json
import random
from kafka import KafkaProducer, KafkaConsumer
from kafka_utility_functions import send_email_simulation


def order_processing_consumer(shutdown_event):
    consumer_5 = KafkaConsumer(
        'e-commerce-orders',
        bootstrap_servers='localhost:9092',
        group_id='order_handling_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode())
    )

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode()
    )

    try:
        while not shutdown_event.is_set():
            messages = consumer_5.poll(timeout_ms=1000)
            if messages:
                for msgs in messages.values():
                    for message in msgs:
                        if random.random() < 0.5:
                            order_id = message.value['order_id']
                            customer_id = message.value['customer_id']
                            send_email_simulation(order_id, customer_id)

                            message.value['processed'] = True
                            producer.send('processed-orders', message.value)
                            producer.flush()

    except Exception as e:
        print(f'Error in order handling consumer: {e}')
    finally:
        consumer_5.close()


""" import threading
shutdown_event = threading.Event()
consumer_output = {}
order_handling_consumer(shutdown_event) """
