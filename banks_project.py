import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

URL = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
CSV_PATH = "Largest_banks_data.csv"
TABLE_ATTRIBUTE = ["Name", "MC_USD_Billion"]
FINAL_TABLE_ATTRIBUTE = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = "code_log.txt"


# Task 1
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(f"<{timestamp}> : <{message}> \n")


log_progress("Preliminaries complete. Initiating ETL process")


# Task 2
def extract():
    response = requests.get(URL)
    html_web = response.text
    html_object = BeautifulSoup(html_web, "html.parser")

    table = html_object.find('table', {'class': 'wikitable'})
    df = pd.DataFrame(columns=TABLE_ATTRIBUTE)

    for row in table.find('tbody').find_all('tr'):
        columns = row.find_all('td')
        if columns:  # Skip header rows
            name = columns[1].text.strip()
            mc_usd_billion = columns[2].text.strip()

            mc_usd_billion = float(mc_usd_billion[:-1]) if mc_usd_billion[-1].isalpha() else float(mc_usd_billion)
            data_dic = {'Name': name, 'MC_USD_Billion': mc_usd_billion}

            df1 = pd.DataFrame(data=data_dic, index=[0])
            df = pd.concat([df1, df], ignore_index=True)
    return df


raw_data = extract()
log_progress("Data extraction complete. Initiating Transformation process")


def transform(df):
    with open("exchange_rate.csv", "r") as file:
        data_exchange = file.read()
        x = data_exchange.split("\n")[1:-1]
        exchange_rate = {}
        for i in x:
            currency, exchange_rate_val = i.split(",")
            exchange_rate[currency] = float(exchange_rate_val)

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    print(df['MC_EUR_Billion'][4])
    return df

transformed_data = transform(raw_data)
print(transformed_data.to_string())
log_progress("Data transformation complete. Initiating Loading process")


def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

load_to_csv(transformed_data, CSV_PATH)
log_progress("Data saved to CSV file")

CONNECTION = sqlite3.connect("Banks.db")
log_progress("SQL Connection initiated")


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


load_to_db(transformed_data, CONNECTION, TABLE_NAME)
log_progress("Data loaded to Database as a table, Executing queries")


def run_query(query_statement, sql_connection):
    result_df = pd.read_sql_query(query_statement, sql_connection)
    return result_df


query_1 = f"SELECT * FROM {TABLE_NAME}"
query_2 = f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}"
query_3 = f"SELECT Name from {TABLE_NAME} LIMIT 5"

result_1 = run_query(query_1, CONNECTION)
result_2 = run_query(query_2, CONNECTION)
result_3 = run_query(query_3, CONNECTION)
#print(result_1)
print("*"*10)
print(result_2)
print("*"*10)
print(result_3)
log_progress("Process Complete")

CONNECTION.close()
log_progress("Server Connection closed")
