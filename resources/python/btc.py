from dotenv import load_dotenv
from requests import Session
from datetime import datetime
import json
import os
import smtplib
import smtplib

load_dotenv('./dags/.env')

limitValue = 130000
sender = 'from@example.com'
receivers = ['to@example.com']
message = """From: From Person <from@example.com>
To: To Person <to@example.com>
Subject: Bitcoin maior


Bitcoin maior que R$ """
message = message + str(limitValue)

COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY')
SMTP_SERVER = os.getenv('SMTP_SERVER')

# Prints log
def print_log(text):
    print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' - ' + text)

def getBitcoinQuotes():
    url = 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest' # Coinmarketcap API url
    parameters = { 
        'symbol': 'BTC',
        'convert': 'BRL'
    }

    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY
    } # Replace 'YOUR_API_KEY' with the API key you have recieved in the previous step

    session = Session()
    session.headers.update(headers)

    print_log('Connecting to api coinmarketcap...')
    response = session.get(url, params=parameters)

    info = json.loads(response.text)
    
    if 'data' in info:
        price = info['data']['BTC'][0]['quote']['BRL']['price']
        print_log("Preço do Bitgcoin: R$ "+str(price))
        if(price < limitValue):
            print_log("Bitcoin é menor que R$ "+str(limitValue))
            try:
                smtpServer = SMTP_SERVER +':1025'
                smtpObj = smtplib.SMTP(smtpServer)
                smtpObj.sendmail(sender, receivers, message)
                print_log("E-mail enviado")
            except:
                print_log("Erro ao enviar o e-mail")
        else:
            print_log("Bitcoin é maior ou igual a R$ "+str(limitValue))
            print_log("E-mail não enviado")
    else :
        if 'error_message' in info['status']:
            print_log(info['status']['error_message'])

getBitcoinQuotes();