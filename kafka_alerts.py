# source ../../Data-Engineering/lecture-5/kafka-project/kafka-env/bin/activate
from pyspark.sql.functions import window, col, avg
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
from configs import kafka_config
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = (SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate())   # Створення SparkSession
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Визначення схеми для JSON,
json_schema = StructType([
    StructField("sensor_ID", IntegerType(), True),
    StructField("timestamp", DoubleType(), True),  # StringType(), IntegerType()
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Читання потоку даних із Kafka (Вказівки, як під'єднуватися, паролі, протоколи:  maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.)
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "vekh__spark_streaming_in") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

# Декодуємо value та конвертуємо у структуру JSON
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")  # Розпаковуємо структуру

# виправлення типу колонки timestamp
df_parsed = df_parsed.withColumn("timestamp", (col("timestamp").cast("timestamp")))
print('df_parsed schema:')
df_parsed.printSchema()  # Перевіряємо схему

# обчислення середніх значень
df_avg = df_parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity"))

print('df_avg schema:')
df_avg.printSchema()  # Перевіряємо

# query_console = df_avg.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", "/tmp/kafka-checkpoints") \
#     .start() \
#     .awaitTermination()

# Завантаження умов алертів із CSV (Читаємо файл alerts_conditions.csv)
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", IntegerType(), True),
    StructField("humidity_max", IntegerType(), True),
    StructField("temperature_min", IntegerType(), True),
    StructField("temperature_max", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True),
])
alerts_df = spark.read.csv("alerts_conditions.csv", schema=alerts_schema, header=True)
print('alerts_df schema:')
alerts_df.printSchema()  # Перевіряємо дані
alerts_df.show()

# Перевірка середніх значень проти умов алертів за cross join, та порівнюємо з умовами alerts_conditions.csv.
df_alerts = df_avg.crossJoin(alerts_df) \
    .filter(
        ((col("humidity_min") == -999) | (col("avg_humidity") >= col("humidity_min"))) &
        ((col("humidity_max") == -999) | (col("avg_humidity") <= col("humidity_max"))) &
        ((col("temperature_min") == -999) | (col("avg_temperature") >= col("temperature_min"))) &
        ((col("temperature_max") == -999) | (col("avg_temperature") <= col("temperature_max")))
    ) \
    .select(col("window").alias("window"), col("avg_temperature").alias("t_avg"), col("avg_humidity").alias("h_avg"), "code", "message") \
    .withColumn("timestamp", current_timestamp())

print('df_alerts:')
df_alerts.printSchema()  # Перевіряємо результати


# # # Виведення даних на екран
# displaying_df = df_alerts.writeStream \
#     .trigger(availableNow=True) \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", "/tmp/checkpoints-2") \
#     .start() \
#     .awaitTermination()


# # Перетворюємо у JSON
df_json = df_alerts.select(to_json(struct("*")).alias("value"))

# Запис алертів у Kafka Надсилаємо результати у vekh__spark_streaming_out:
df_json.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("topic", "vekh__spark_streaming_out") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start() \
    .awaitTermination()