# from kafka import KafkaConsumer

# # consumer = KafkaConsumer('Test')

# consumer = KafkaConsumer(
#     'Test', 
#     bootstrap_servers=['localhost:9092'],
#     # value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#     enable_auto_commit=True,
#     auto_offset_reset='latest',
#     max_poll_records=1,
#     max_poll_interval_ms=300000,
#     consumer_timeout_ms=300000,
# )


# for msg in consumer:
#     print(msg, msg.value.decode('utf-8'))


from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'g1',
    'auto.offset.reset': 'latest'
})

c.subscribe(['Test'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()