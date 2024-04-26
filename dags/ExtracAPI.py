import requests

import pandas as pd
from datetime import date

import psycopg2
import os 

from psycopg2.extras import execute_values

#Obtengo los datos de la API y los visualizo con PANDAS
def Extraer_data():
    url = "https://api.coinlore.net/api/tickers/?start=0&limit=15"
    url2 = "https://api.coinlore.net/api/tickers/?start=85&limit=15" #las ultimas 15 cryptomonedas del top 100
    response1 = requests.get(url)
    datos=response1.json()

    response2 = requests.get(url2)
    datos2= response2.json()

    for i in datos2['data']:
        datos['data'].append(i)  
        
    df=pd.DataFrame.from_dict(datos['data']).reset_index().rename(columns={"index":'indice'})
    

    hora_actual = pd.to_datetime('now')
    hora_formateada = hora_actual.strftime("%D %H:%M:%S")
    
    df_filtrado= df.loc[0:15,['indice','id','symbol','name','rank','price_usd','percent_change_24h','percent_change_1h','market_cap_usd']]

    df_filtrado =df_filtrado.assign(Extraction_time =hora_formateada)

    #Cambio los tipos de datos
    df_filtrado["price_usd"]= df_filtrado["price_usd"].astype("float64")

    df_filtrado["percent_change_24h"]= df_filtrado["percent_change_24h"].astype("float64")

    df_filtrado["percent_change_1h"]= df_filtrado["percent_change_1h"].astype("float64")

    df_filtrado["market_cap_usd"]= df_filtrado["market_cap_usd"].astype("float64")
    
    df_filtrado=df_filtrado.to_dict()
    
    return df_filtrado



#Conexion con Amazon redshift
url= "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base= "data-engineer-database"
user= "luchitrading_coderhouse"
pwd= "6vJ8t1V7Tp"

def conexion_tabla():
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conectado a Postgres")
        
    except Exception as e:
        print("No es posible conectarse a Postgres")
        print(e)

#Creacion de tabla en redshift
def cargar_en_postgres():
    df_filtrado= Extraer_data()
    
    dataframe=pd.DataFrame(df_filtrado)
    table_name= "ValoresCryptos"
    conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
    dtypes= dataframe.dtypes
    cols= list(dtypes.index )
    tipos= list(dtypes.values)
    type_map = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)','bool':'BOOLEAN'}
    sql_dtypes = [type_map[str(dtypes)] for dtypes in tipos]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    # Crear la tabla
    cur = conn.cursor()
    cur.execute(table_schema)
    # Generar los valores a insertar
    values = [tuple(x) for x in dataframe.to_numpy()]
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')


"""#Subida de datos a la base de datos
Extraer_data()
conexion_tabla()
cargar_en_postgres()"""

