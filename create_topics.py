# Наведений код створює новий топік з трьома партиціями та коефіцієнтом реплікації 1 в Apache Kafka, використовуючи конфігурацію з файлу kafka_config

from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config
    # KafkaAdminClient — клас для взаємодії з адміністративними функціями Kafka;
    # NewTopic — клас для визначення нового топіку;
    # kafka_config — конфігураційний файл, що містить налаштування Kafka.

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)
    # bootstrap_servers — список серверів Kafka;
    # security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password — параметри для налаштування безпеки та автентифікації.

topics = ["spark_streaming_in", "spark_streaming_out"]
my_name = "vekh"
for topic in topics:
    # Визначення нового топіку
    # my_name = "oleksiy"
    topic_name = f'{my_name}__{topic}'
    num_partitions = 2
    replication_factor = 1
        # my_name — ім'я користувача, яке використовується для створення імені топіку;
        # topic_name — назва нового топіку;
        # num_partitions — кількість партицій у топіку;
        # replication_factor — коефіцієнт реплікації для забезпечення стійкості даних (ми будемо використовувати тільки значення 1);
        # new_topic — об'єкт нового топіку, який визначає його параметри
    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    # Створення нового топіку
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
            # try та except використовуються для обробки помилок при створенні топіку;
            # admin_client.create_topics(new_topics=[new_topic], validate_only=False) — команда для створення топіку. Якщо validate_only встановити у True, то топік не буде створений, а лише буде перевірено його визначення.

# Перевіряємо список існуючих топіків
# print(admin_client.list_topics())

for topic in admin_client.list_topics():
    if "vekh_" in topic:
        print(topic)

# Закриття зв'язку з клієнтом
admin_client.close()