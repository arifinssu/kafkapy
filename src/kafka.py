from confluent_kafka import (Producer, Consumer, KafkaError, KafkaException)

class Kafka():
    def __init__(self, config):
        self.consumer = Consumer(config)
        self.producer = Producer(config)
        self.running = False
        self.topics = None
        self.callback = None

    def subscribe(self, topics):
        self.topics = topics

    def loop(self):
        self.consumer.subscribe(self.topics)

        while self.running:
            message = self.consumer.poll(timeout=1)
            if message is None: continue

            print(message.value().decode("utf-8"))
            # if message.error().code() == KafkaError._PARTITION_EOF:
            #     sys.stderr.write(
            #         '%% %s [%d] reached end at offset %d\n' %(message.topic(), message.partition(), message.offset())
            #     ) # eo partition event

            # elif message.error():
            #     raise KafkaException(message.error())

            # else:
            #     print(message.value().decode("utf-8"))

        self.consumer.close()

    def set_delivery_callback(self, callback):
        self.callback = callback

    def shutdown(self):
        self.running = False

    def startup(self):
        self.running = True

    def publish(self, message):
        if self.callback is not None:
            return self.producer.produce(self.topics, message, self.callback)
        return self.producer.produce(self.topics, message)

    def flush(self):
        return self.producer.flush()