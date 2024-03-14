import requests

url = "https://api.coinlore.net/api/tickers/?start=0&limit=100"
response = requests.get(url)
datos= response.json()

import pandas as pd
from datetime import date
df=pd.DataFrame.from_dict(datos['data'])
df

url-sql= data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
data_base=data-engineer-database
user=luchitrading_coderhouse
pwd=6vJ8t1V7Tp