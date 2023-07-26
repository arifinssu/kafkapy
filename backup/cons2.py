from confluent_kafka import Consumer, KafkaError, KafkaException

# consumer = KafkaConsumer(CONSUMER_TOPIC, 
#     group_id='bar',
#     bootstrap_servers=[f"{KAFKA_SERVER_HOST}:{KAFKA_SERVER_PORT}"],
#     value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#     enable_auto_commit=True,
#     auto_offset_reset='latest',
#     max_poll_records=1,
#     max_poll_interval_ms=300000,
#     consumer_timeout_ms=300000
# )

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bar',
    'auto.offset.reset': 'latest'
})

running = True

def loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # msg_process(msg)
                print(msg.value().decode("utf-8"))

    finally:
        consumer.close()

def shutdown():
    running = False

def startup():
    running = True

loop(consumer, ["Test"])