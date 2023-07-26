# running machine

from src.kafka import Kafka

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bar',
    'auto.offset.reset': 'latest'
}

c = Kafka(conf)
c.subscribe(["Test"])
c.running = False
while True:
    c.loop()