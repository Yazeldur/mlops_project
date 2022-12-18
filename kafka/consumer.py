from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import pandas as pd
from atm_dataclass import Data

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

list_date = []
list_id = []
list_argent_retire = []
csv_counter = 0

for message in consumer:
    message = message.value
    message = Data.schema().loads(message)
    if len(list_date) == 20000:
        print("creating csv...")
        df=pd.DataFrame(list(zip(list_date, list_id, list_argent_retire)), columns=['date', 'id', 'argent_retire'])
        df.to_csv(f"../atm_csvs/atm{csv_counter}.csv")
        csv_counter += 1
        list_date = []
        list_id = []
        list_argent_retire = []
    list_date.append(message.date)
    list_id.append(message.id)
    list_argent_retire.append(message.argent_retire) 
    print(message)