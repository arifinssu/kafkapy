from src.kafka import Kafka

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'bar',
    'auto.offset.reset': 'latest'
}

p = Kafka(conf)
p.subscribe("Test")
p.publish("bre")
p.flush()