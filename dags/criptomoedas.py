from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv
from requests import Session
from datetime import datetime
import time
import os
import oracledb
import json

with DAG(
    'criptomoedas',
    start_date=days_ago(0),
    schedule_interval='@daily'
) as dag:

    load_dotenv()

    ORACLE_WALLET_DIRECTORY = os.getenv('ORACLE_WALLET_DIRECTORY')
    ALPHAVANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
    COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY')
    
    coins = ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'MATIC', 'LTC', 'ETC']

    # Prints log
    def print_log(text):
        print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' - ' + text)

    # Getting quotes
    def getQuotes(coin):
        url = 'https://www.alphavantage.co/query'
        
        parameters = { 
            'function': 'DIGITAL_CURRENCY_DAILY',
            'symbol': coin,
            'market': 'USD',
            'apikey': ALPHAVANTAGE_API_KEY
        }

        headers = {
            'Accept': 'application/json'
        }

        session = Session()
        session.headers.update(headers)

        print_log('Connecting to api alphavantage ('+coin+')...')
        response = session.get(url, params=parameters)

        info = json.loads(response.text)
        if 'Error Message' in info:
            print_log(info['Error Message'])
            return []
        return info

    # Connects to oracle
    def getOracleConnection():
        return oracledb.connect(
            user="admin",
            password="Impacta123456",
            dsn="dbcryptos_high",
            config_dir=ORACLE_WALLET_DIRECTORY,
            wallet_location=ORACLE_WALLET_DIRECTORY,
            wallet_password="Impacta123456"
        )

    # Insert coin into db
    def insertCoinDb(connection, coin_id, name, symbol, image):
        print_log('Inserting database into table COINS...')
        with connection.cursor() as cursor:
            cursor.execute("UPDATE COINS SET NAME = :1, IMAGE = :2, UPDATED_DATE = :3 WHERE ID = :4", [name, image, datetime.now(), coin_id])
            print_log(str(cursor.rowcount) + " rows updated into COINS table")
            if (cursor.rowcount == 0):
                cursor.execute("INSERT into COINS (ID, NAME, IMAGE, CREATION_DATE) VALUES(:1, :2, :3, :4)", [coin_id, name, image, datetime.now()])
                print_log(str(cursor.rowcount) + " rows inserted into COINS table")

    # Insert quote into db
    def insertQuoteDb(connection, date, coin_id, open, close, high, low, volume, marketCap):
        print_log('Inserting database into table QUOTES...')
        with connection.cursor() as cursor:
            cursor.execute("UPDATE QUOTES SET OPEN_VALUE = :1, CLOSE_VALUE = :2, HIGH_VALUE = :3, LOW_VALUE = :4, VOLUME = :5, MARKET_CAP = :6, UPDATED_DATE = :7 WHERE HISTORICAL_DATE = TO_DATE(:8, 'YYYY-MM-DD') AND CRYPTO_ID = :9", [open, close, high, low, volume, marketCap, datetime.now(), date, coin_id])
            print_log(str(cursor.rowcount) + " rows updated into QUOTES table")
            if (cursor.rowcount == 0):
                cursor.execute("INSERT into QUOTES (HISTORICAL_DATE, CRYPTO_ID, OPEN_VALUE, CLOSE_VALUE, HIGH_VALUE, LOW_VALUE, VOLUME, MARKET_CAP, CREATION_DATE) VALUES(TO_DATE(:1, 'YYYY-MM-DD'), :2, :3, :4, :5, :6, :7, :8, :9)", [date, coin_id, open, close, high, low, volume, marketCap, datetime.now()])
                print_log(str(cursor.rowcount) + " rows inserted into QUOTES table")

    def getImageCoin():
        coinsConcatenated = ','.join(coins)
        url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/info' # Coinmarketcap API url
        parameters = { 
            'symbol': coinsConcatenated,
            'aux': 'logo'
        }

        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY
        }

        session = Session()
        session.headers.update(headers)

        print_log('Connecting to api coinmarketcap...')
        response = session.get(url, params=parameters)

        info = json.loads(response.text)
        if info['status']['error_code'] != 0:
            print_log(info['status']['error_message'])
            return []
        return info['data']

    def saveImageUrlDb(connection, info):
        print_log('Inserting database into table COINS...')
        for u in info:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE COINS SET IMAGE = :1, UPDATED_DATE = :2 WHERE ID = :3", [info[u][0]['logo'], datetime.now(), info[u][0]['symbol']])
                print_log(str(cursor.rowcount) + " rows updated into COINS table")

    # Getting historical quotes
    def getHistorical():
        connection = getOracleConnection()
        count = 1
        try:
            for c in coins:
                info = getQuotes(c)
                if 'Meta Data' in info:
                    name = info['Meta Data']['3. Digital Currency Name']
                    symbol = info['Meta Data']['2. Digital Currency Code']
                    insertCoinDb(connection, symbol, name, '', '')
                    for i in info['Time Series (Digital Currency Daily)']:
                        print_log("Inserting " + i + " quotes to " + name)
                        open = info['Time Series (Digital Currency Daily)'][i]['1b. open (USD)']
                        close = info['Time Series (Digital Currency Daily)'][i]['4b. close (USD)']
                        high = info['Time Series (Digital Currency Daily)'][i]['2b. high (USD)']
                        low = info['Time Series (Digital Currency Daily)'][i]['3b. low (USD)']
                        volume = info['Time Series (Digital Currency Daily)'][i]['5. volume']
                        marketCap = info['Time Series (Digital Currency Daily)'][i]['6. market cap (USD)']
                        insertQuoteDb(connection, i, symbol, open, close, high, low, volume, marketCap)
                if((count%5) == 0): # 5 API requests per minute and 500 requests per day
                    print_log("Waiting...")
                    time.sleep(70) #Waits
                count += 1
            print_log("Updating logo's coins..")
            images = getImageCoin()
            saveImageUrlDb(connection, images)
            connection.commit()
        except:
            print_log("Erro!!")
            connection.rollback()
        connection.close()

    tarefa = PythonOperator(
        task_id='getHistorical',
        python_callable=getHistorical
    )
