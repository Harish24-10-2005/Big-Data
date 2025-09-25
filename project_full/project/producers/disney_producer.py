from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

titles = pd.read_csv('/home/hadoop/project/data/disney-tv-shows-and-movies/titles.csv')
credits = pd.read_csv('/home/hadoop/project/data/disney-tv-shows-and-movies/credits.csv')
titles['platform'] = 'Disney'
titles['ingestion_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

credits['platform'] = 'Disney'
credits['ingestion_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


for i, (_, row) in enumerate(titles.iterrows(), 1):
    producer.send('ott_titles', row.to_dict())
    if i == 10: break
    time.sleep(0.01)

# Stream credits (fixed bug: using credits, not titles)
for i, (_, row) in enumerate(credits.iterrows(), 1):
    producer.send('ott_credits', row.to_dict())
    if i == 10: break
    time.sleep(0.01)


producer.flush()
print("Disney Titles & Credits sent to Kafka!")

