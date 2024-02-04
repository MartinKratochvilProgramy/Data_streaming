from kafka import KafkaConsumer

# Specify the Kafka broker(s) and the topic
kafka_bootstrap_servers = 'localhost:9092, localhost:9093, localhost:9094'
kafka_topic = 'random_names'

consumer = KafkaConsumer(
    kafka_topic,
    group_id='my_consumer_group',
    bootstrap_servers=kafka_bootstrap_servers,
    auto_offset_reset='earliest'  # Start consuming from the beginning of the topic
)

try:
    # Continuously poll for new messages
    for message in consumer:
        # Process the received message
        print(f"Received message: {message.value.decode('utf-8')}")

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer.close()
