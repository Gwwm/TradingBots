import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
import math

from config import api_key, api_secret
from func.stablecoin import stablecoin
from func.notify import send_slack_message
import threading
import csv

import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
from main import get_symbols, get_balance, convert_min_step_size

client = Client(api_key, api_secret)

target_list = [
    'BURGERUSDT',
    'BAKEUSDT',
    'TKOUSDT',
    'FORTHUSDT',
    'SLPUSDT',
    'SUPERUSDT',
    'DNTUSDT',
    'DEGOUSDT',
    'TLMUSDT',
    'REEFUSDT',
]

def regress_signal(symbol):
    try:
        latest_record = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_5MINUTE, limit=25)

        columns = ['Open Time',
                   'Open',
                   'High',
                   'Low',
                   'Close',
                   'Volume',  # volume in coins
                   'Close Time',
                   'Quote asset volume',
                   'Number of Trade',
                   'Taker buy base vol',
                   'Take buy quote vol',
                   'ignore']

        if len(latest_record) < 24:
            print("not enought klines")
            return 1

        if datetime.fromtimestamp(latest_record[-1][0]/1000) < datetime.now() - timedelta(minutes=5):
            print("legacy transaction history")
            return 1

        latest_record = latest_record[:-1]

        df = pd.DataFrame(latest_record, columns=columns)

        df['Volume'] = df['Volume'].apply(float)
        df['Close'] = df['Close'].apply(float)
        df['High'] = df['High'].apply(float)
        df['Low'] = df['Low'].apply(float)

        x = sm.add_constant(df.index)
        df['tp'] = (df['High'] + df['Low'] + df['Close']) / 3
        y = df['tp']
        window = 12 * 2
        rols = RollingOLS(y, x, window=window)
        rres = rols.fit()

        params = rres.params
        params.columns = [f"const_{window}", f"beta_{window}"]

        rsquared = rres.rsquared
        rsquared.name = f'r2_{window}'

        df = pd.concat([df, params, rsquared], axis=1)

        df['buy_significance'] = df[f'r2_{window}'] > 0.7
        df['buy_beta'] = df[f'beta_{window}'] < 0
        df['buy_signal'] = df['buy_beta'] & df['buy_significance']

        window = 12
        rols = RollingOLS(y[-12:], x[-12:], window=window)
        rres = rols.fit()

        params = rres.params
        params.columns = [f"const_{window}", f"beta_{window}"]

        rsquared = rres.rsquared
        rsquared.name = f'r2_{window}'

        df = pd.concat([df, params, rsquared], axis=1)

        df['sell_significance'] = df[f'r2_{window}'] > 0.7
        df['sell_beta'] = df[f'beta_{window}'] > 0
        df['sell_signal'] = df['sell_beta'] & df['sell_significance']

        latest_entry = df.iloc[[-1]]

        if latest_entry['buy_signal'].item():
            latest_price = latest_entry['Close'].item()

            if symbol in target_list:
                # transaction execution
                target_asset = symbol.replace('USDT', '')
                if not get_balance(target_asset):
                    balance = get_balance('USDT')

                    if balance:
                        if balance < 100:
                            target_cost = balance
                        else:
                            target_cost = 100

                        order_book = client.get_order_book(symbol=symbol)
                        limit_price = order_book['asks'][0][0]

                        quantity = target_cost / float(limit_price)
                        quantity = convert_min_step_size(symbol=symbol, quantity=quantity)

                        response = client.order_limit_buy(symbol=symbol, quantity=quantity, price=limit_price)
                        send_slack_message(str(response))
                        send_slack_message(f"REGRESS BUY signal {symbol}, {datetime.now().strftime('%H:%M:%S')}, price: {latest_price}")


            with open(f"{symbol}-regress-signal.csv", 'a') as file:
                csvwriter = csv.writer(file)
                csvwriter.writerow([datetime.now().strftime('%H:%M:%S'), 'BUY', latest_price])


        elif latest_entry.sell_signal.item():
            latest_price = latest_entry['Close'].item()

            if symbol in target_list:
                target_asset = symbol.replace('USDT', '')
                balance = get_balance(target_asset)
                balance = convert_min_step_size(symbol=symbol, quantity=balance)

                if balance:
                    order_book = client.get_order_book(symbol=symbol)
                    limit_price = order_book['bids'][0][0]
                    response = client.order_limit_sell(symbol=symbol, quantity=balance, price=limit_price)
                    send_slack_message(f"REGRESS SELL signal {symbol}, {datetime.now().strftime('%H:%M:%S')}, price: {latest_price}")

            with open(f"{symbol}-regress-signal.csv", 'a') as file:
                csvwriter = csv.writer(file)
                csvwriter.writerow([datetime.now().strftime('%H:%M:%S'), 'SELL', latest_price])

        return 0

    except Exception as error:
        print(f"{symbol} exception {error}")
        return 1


if __name__ == '__main__':
    symbols = get_symbols()

    print(f"{datetime.now().strftime('%H:%M:%S')} start")
    threads = []

    for symbol in symbols:
        t = threading.Thread(target=regress_signal, args=(symbol, ))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"{datetime.now().strftime('%H:%M:%S')} completed")