Extraccion de datos de una API sobre valores de Cryptomonedas
En este proyecto se extraen datos de la API "https://api.coinlore.net/api/tickers/", se filtran y agregan en una tabla en redshift
Se utilizan las siguientes librerias:
  -Requests
  -Pandas
  -Datetime
  -Psycopg2
  -os
  -psycopg2.extras
  -json
Se instalan usando el archivo requirements.txt. 
Dentro de la terminal, pip install -r requirements.txt

Para poder conectarse a redshift se debera completar los datos de la url, data-base, user y pwd