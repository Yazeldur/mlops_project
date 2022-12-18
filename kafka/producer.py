from time import sleep
from json import dumps
import json
from kafka import KafkaProducer
from atm_dataclass import Data

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

# Opening JSON file
json_file = open('../data_generation/data.json')
data = json_file.read()
siu = Data.schema().loads(data, many=True)

for data_obj in siu:
    producer.send('numtest', value=data_obj.to_json()) #object of 3 fields
    sleep(0.01)

json_file.close()












#in home/kafka_2.13-3.2.0/bin : ./zookeeper-server-start.sh ../config/zookeeper.properties
#                          ./kafka-server-start.sh ../config/server.properties
#                          kafka-topics.sh --bootstrap-server localhost:9092 --topic numtest --create --partitions 1 --replication-factor 1
#                          lance producer.py sur 1 terminal, puis consumer.py sur un autre terminal