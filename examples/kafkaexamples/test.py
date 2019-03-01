from kafka import KafkaConsumer

consumer = KafkaConsumer("some-topic", bootstrap_servers=["localhost:9092"])
