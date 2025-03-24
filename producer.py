# імітує роботу датчика і періодично відправляє випадково згенеровані дані (температура та вологість) у топік building_sensors

#  source ../../Data-Engineering/lecture-5/kafka-project/kafka-env/bin/activate

from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)


topic_name ="vekh__spark_streaming_in"
sensor_ID = random.randint(100, 999)
for i in range(60):
    # Відправлення повідомлення в топік
    try:
        data = {
            "sensor_ID": sensor_ID,
            "timestamp": time.time(),  # Часова мітка
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85)  # Випадкове значення
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{topic_name}' successfully.")
        time.sleep(10)
    except Exception as e:
        print(f"An error occurred: {e}")


producer.close()  # Закриття producer / Закриття з'єднання з Kafka після відправлення всіх повідомлень.



