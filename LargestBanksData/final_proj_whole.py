from datetime import datetime 
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3
import os






def log_progress(message):
    global overwrite_flag
    now=datetime.now()
    timestamp=now.strftime('%Y-%h-%d %H-%M-%S')
    with open(log_file,'a') as f:
        f.write(f'{timestamp} : {message} \n')
     

def extract(url,table_attr):
    r=requests.get(url).text
    soup=BeautifulSoup(r,'html.parser')
    df=pd.DataFrame(columns=table_attr)
    tables=soup.find_all("tbody")
    rows=tables[0].find_all('tr')
    for row in rows:
        col=row.find_all('td')
        if len(col)!=0:
            if col[0] is not None:
                dicti={"Name":col[1].get_text(strip=True),
                   "MC_USD_Billion":col[2].get_text(strip=True)}
                df2=pd.DataFrame(dicti,index=[0])
                df=pd.concat([df,df2],ignore_index=True)
    return df



def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    from_file=pd.read_csv(csv_path)
    dictionary={from_file.loc[0,'Currency']:df.iloc[:,1].apply(lambda x: float(x)*from_file.loc[0,'Rate']),
                from_file.loc[1,'Currency']:df.iloc[:,1].apply(lambda x: float(x)*from_file.loc[1,'Rate']),
                from_file.loc[2,'Currency']:df.iloc[:,1].apply(lambda x: float(x)*from_file.loc[2,'Rate']),}
    df3=pd.DataFrame(dictionary)
    df=pd.concat([df,df3],axis=1)


    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement,sql_connection))


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
attrs=['Name','MC_USD_Billion']
path=f'{os.getcwd()}/Largest_banks_data.csv'
db_name='Banks.db'
table_name='Largest_banks'
log_file=f'{os.getcwd()}/code_log.txt'

log_progress('Preliminaries complete. Initiating ETL process')



df=extract(url,attrs)
log_progress('Data extraction complete. Initiating Transformation process')

df=transform(df,f'{os.getcwd()}/exchange_rate.csv')

log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df,path)

log_progress('Data saved to CSV file')

conn=sqlite3.connect(db_name)


log_progress('SQL Connection initiated')

load_to_db(df,conn,table_name)

log_progress('Data loaded to Database as a table, Executing queries')

run_query(f"SELECT * FROM {table_name}",conn)

log_progress('Process Complete')

conn.close()

log_progress('Server Connection closed')








