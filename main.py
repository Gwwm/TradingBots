import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
import math

from config import api_key, api_secret
from func.stablecoin import stablecoin
from func.notify import send_slack_message
import threading

import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS

import json

# python binance
client = Client(api_key, api_secret)


def get_symbols():
    res = client.get_ticker()
    symbols = []
    for symbol in res:
        if symbol['symbol'][-4:] == 'USDT':
            if not any(coin in symbol['symbol'][:-4] for coin in stablecoin):
                if not any(dev in symbol['symbol'][:-4] for dev in ['BULL', 'BEAR', 'UP', 'DOWN']):
                    symbols.append(symbol['symbol'])

    return symbols


def convert_min_step_size(symbol, quantity):
    symbol_info = client.get_symbol_info(symbol)
    step_size = 0.0
    for f in symbol_info['filters']:
        if f['filterType'] == 'LOT_SIZE':
            step_size = float(f['stepSize'])

    precision = int(round(-math.log(step_size, 10), 0))
    tradable_amount = round(math.floor(quantity * 100) / 100, precision)

    return tradable_amount


def get_balance(asset):
    balance_info = client.get_asset_balance(asset)
    balance = float(balance_info['free'])
    if asset == 'USDT':
        asset_usdt_value = balance
    else:
        current_price = float(client.get_avg_price(symbol=f"{asset}USDT")['price'])
        asset_usdt_value = balance * current_price

    if asset_usdt_value > 10:
        return balance
    else:
        return False


def signal_generator(symbol):
    start = str((datetime.now() - timedelta(days=7.5)).timestamp() * 1000)
    end = str(datetime.now().timestamp() * 1000)

    latest_record = client.get_historical_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_5MINUTE,
                                                 start_str=start, end_str=end)

    columns = ['Open Time',
               'Open',
               'High',
               'Low',
               'Close',
               'Volume',  # coin volume
               'Close Time',
               'Quote asset volume',
               'Number of Trade',
               'Taker buy base vol',
               'Take buy quote vol',
               'ignore']

    df = pd.DataFrame(latest_record, columns=columns)

    df['Volume'] = df['Volume'].apply(float)
    df['Close'] = df['Close'].apply(float)
    df['datetime'] = df.apply(lambda x:
                              datetime.fromtimestamp(x['Close Time'] / 1000).strftime('%Y:%m:%d %H:%M:%S'), axis=1)
    df['weekly_vol_mean'] = df['Volume'].rolling(7 * 12 * 24, min_periods=7 * 12 * 24).mean().shift(1)
    df['hourly_vol_mean'] = df['Volume'].rolling(12, min_periods=12).mean().shift(1)

    df['prev_price'] = df['Close'].shift(1)
    df['ptc'] = df['Close'] / df['prev_price'] - 1

    df['quote_asset_vol_mean'] = df['Quote asset volume'].rolling(12 * 24 * 7).mean()
    df['market_cap_signal'] = df['quote_asset_vol_mean'] > 100000
    df['buy_vol_signal'] = df['Volume'] > df['weekly_vol_mean'] * 5
    df['buy_price_signal'] = df['ptc'] > 0.0075

    df['sell_vol_below_mean_signal'] = df['Volume'] < df['hourly_vol_mean'] * 0.5
    df['sell_vol_above_mean_signal'] = df['Volume'] > df['hourly_vol_mean'] * 5
    df['sell_vol_signal'] = df['sell_vol_below_mean_signal'] | df['sell_vol_above_mean_signal']
    df['sell_price_signal'] = df['ptc'] < -0.005

    df['absolute_sell_signal'] = df['ptc'] < -0.075
    df['sell_signal'] = (df['sell_price_signal'] & df['sell_vol_signal']) | df['absolute_sell_signal']
    df['rebound'] = [any(window.to_list()) for window in df.sell_signal.rolling(window=4)]
    df['buy_signal'] = df['buy_price_signal'] & df['buy_vol_signal'] & df['market_cap_signal'] & ~df['rebound']

    if df.iloc[[-2]]['buy_signal'].item():
        current_price = df.iloc[[-1]]['Close'].item()
        target_asset = symbol.replace('USDT', '')
        send_slack_message(f"BUY signal {symbol}, {datetime.now().strftime('%H:%M:%S')}, price: {current_price}")

        # does not have the asset at the moment
        if not get_balance(target_asset):
            balance = get_balance('USDT')

            if balance > 50:
                order_book = client.get_order_book(symbol=symbol)
                limit_price = order_book['asks'][0][0]

                if balance > 1000:
                    target_cost = balance / 5

                elif 200 > balance > 50:
                    target_cost = balance

                else:
                    target_cost = 200
                quantity = target_cost / float(limit_price)
                quantity = convert_min_step_size(symbol=symbol, quantity=quantity)

                try:
                    response = client.order_limit_buy(symbol=symbol, quantity=quantity, price=limit_price)
                    send_slack_message(str(response))
                except Exception as err:
                    send_slack_message(err)
                    print(f"{err}, {symbol}, BUY, price: {limit_price}, quantity: {quantity}")

        df.to_csv(f"{symbol}-{datetime.now().strftime('%H:%M:%S')}.csv", index=False)

    if df.iloc[[-2]]['sell_signal'].item():
        current_price = df.iloc[[-1]]['Close'].item()

        # has balance
        target_asset = symbol.replace('USDT', '')
        quantity = get_balance(target_asset)
        quantity = convert_min_step_size(symbol=symbol, quantity=quantity)
        current_time = datetime.now().strftime('%H:%M:%S')
        send_slack_message(f"SELL signal {symbol}, {current_time}, PRICE: {current_price}, QUANTITY: {quantity}")

        if quantity:
            try:
                order_book = client.get_order_book(symbol=symbol)
                limit_price = order_book['bids'][0][0]

                response = client.order_limit_sell(symbol=symbol, quantity=quantity, price=limit_price)
                send_slack_message(str(response))

            except Exception as err:
                send_slack_message(err)
                print(f"{err}, {symbol}, SELL quantity: {quantity}")
                sell_resubmission(symbol, limit_price)

        df.to_csv(f"{symbol}-{datetime.now().strftime('%H:%M:%S')}.csv", index=False)

    df.to_csv(f"testing/{symbol}-{datetime.now().strftime('%H:%M:%S')}.csv")


def sell_resubmission(symbol, price, count=1):
    target_asset = symbol.replace('USDT', '')
    balance = get_balance(target_asset)
    quantity = convert_min_step_size(symbol, balance)

    try:
        response = client.order_limit_sell(symbol=symbol, quantity=quantity, price=price)
        print(response)
        send_slack_message("order resubmitted")

    except:
        print(f"failed placing sell order for {symbol}, error count: {count}")
        if count < 6:
            sell_resubmission(symbol, price, count + 1)

    return True


def vol_signal_generator(symbol):
    end = str(datetime.now().timestamp() * 1000)
    start = str((datetime.now() - timedelta(days=1.5)).timestamp() * 1000)

    latest_record = client.get_historical_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1MINUTE,
                                                 start_str=start, end_str=end)

    if len(latest_record) < 60 * 24:
        return 1

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

    df = pd.DataFrame(latest_record, columns=columns)

    df['Volume'] = df['Volume'].apply(float)
    df['Close'] = df['Close'].apply(float)
    df['Open'] = df['Open'].apply(float)
    df['daily_vol_mean'] = df['Volume'].rolling(60 * 24, min_periods=60 * 24).mean().shift(1)
    df['hourly_vol_mean'] = df['Volume'].rolling(60, min_periods=60).mean().shift(1)

    df['vol_signal_1'] = df['Volume'] > df['hourly_vol_mean'] * 30
    df['vol_signal_2'] = df['Volume'] > df['daily_vol_mean'] * 10

    df['price_up'] = df['Close'] > df['Open']

    df['vol_signal'] = df['vol_signal_1'] & df['vol_signal_2'] & df['price_up']

    try:
        if df.iloc[[-2]]['vol_signal'].item():
            send_slack_message(f"{symbol} has unusual transaction volume")

    except Exception as err:
        print(err, symbol)

    return 0


if __name__ == '__main__':
    symbols = get_symbols()

    print(f"{datetime.now().strftime('%H:%M:%S')} start")
    threads = []

    for symbol in symbols:
        t = threading.Thread(target=vol_signal_generator, args=(symbol, ))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"{datetime.now().strftime('%H:%M:%S')} completed")

