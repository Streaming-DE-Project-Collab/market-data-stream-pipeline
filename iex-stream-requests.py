from iexfinance.stocks import Stock, get_historical_data
from kafka import KafkaProducer
import json
import os
import time
from datetime import datetime
iex_token = os.getenv('IEX_TOKEN')

output_format = 'json' # or pandas
symbols = ["TSLA", "AAPL", "GOOG"]
call_time_limit = 0.5 # in seconds

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'stock_prices'

#aapl = Stock('AAPL', output_format='json', token = iex_token)

while True:
    start_time = time.time()
    #print("start_time", datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'))
    for s in symbols: 
        result = Stock(s, output_format = output_format)
        price = result.get_price()
        print(s, price)
        #print(result.get_key_stats())
        producer.send(topic_name, {'symbol': s, 'price': price, 'timestamp': datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')})
    while time.time() - start_time < call_time_limit:
        time.sleep(1)
    end_time = time.time()
    print(f"Time taken to execute: {round(end_time - start_time,2)} seconds")
