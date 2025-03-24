# обробляє отримані повідомлення й виводить їх на екран.

from kafka import KafkaConsumer
from configs import kafka_config
import json


# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='my_consumer_group_4'   # Ідентифікатор групи споживачів
)

# Підписка на теми
consumer.subscribe(["vekh__spark_streaming_out"])
print(f"Subscribed to topics")

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f"Received ALERT: {message.value} , partition {message.partition}")


        # if message.value["temperature"] > 40:
        #     print("    A L E R T :     TEMPERATURE TOO HIGH!!!")
        # if message.value["humidity"] > 80:
        #     print("    A L E R T :     HUMIDITY TOO HIGH!!!")
        # if message.value["humidity"] < 20:
        #     print("    A L E R T :     HUMIDITY TOO LOW!!!")
    #
    # for message in consumer:
    #     print(f"Received ALERT: {message.value} , partition {message.partition}")
    #     if message.value["temperature"] > 40:
    #         print("    A L E R T :     TEMPERATURE TOO HIGH!!!")
    #     if message.value["humidity"] > 80:
    #         print("    A L E R T :     HUMIDITY TOO HIGH!!!")
    #     if message.value["humidity"] < 20:
    #         print("    A L E R T :     HUMIDITY TOO LOW!!!")


except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

