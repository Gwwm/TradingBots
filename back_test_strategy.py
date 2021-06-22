import pandas as pd
from main import client, Client, get_symbols, send_slack_message
from datetime import datetime, timedelta
import os

def get_history(symbol, start='20120101', end='20210430'):
    start_str = str(datetime.strptime(start, "%Y%m%d").timestamp()*1000)
    end_str = str(datetime.strptime(end, "%Y%m%d").timestamp()*1000)

    candles = client.get_historical_klines(symbol=symbol,
                                           interval=Client.KLINE_INTERVAL_5MINUTE,
                                           start_str=start_str, end_str=end_str)
    columns = ['Open Time',
               'Open',
               'High',
               'Low',
               'Close',
               'Volume', # coin volume
               'Close Time',
               'Quote asset volume',
               'Number of Trade',
               'Taker buy base vol',
               'Take buy quote vol',
               'ignore']

    df = pd.DataFrame(candles, columns=columns)
    df.drop(['ignore'], axis=1, inplace=True)

    df['Open'] = df['Open'].apply(float)
    df['High'] = df['High'].apply(float)
    df['Low'] = df['Low'].apply(float)
    df['Close'] = df['Close'].apply(float)
    df['Volume'] = df['Volume'].apply(float)
    df['Quote asset volume'] = df['Quote asset volume'] .apply(float)
    df['datetime'] = df.apply(lambda x:
                              datetime.fromtimestamp(x['Open Time'] / 1000).strftime('%Y/%m/%d %H:%M:%S'), axis=1)

    df.to_csv(f'dataset/{symbol}_5mins.csv', index=False)

    return df

def binance_analystic(symbol, start_date, end_date, sell_window=3):
    if f'{symbol}_5mins.csv' in os.listdir('../dataset'):
        df = pd.read_csv(f"dataset/{symbol}_5mins.csv")
    else:
        df = get_history(symbol)

    start_datetime = datetime.strptime(start_date, '%Y%m%d')
    end_datetime = datetime.strptime(end_date, '%Y%m%d')

    start_timestamp = start_datetime.timestamp()*1000
    end_datetime = end_datetime.timestamp() * 1000

    df = df[df['Open Time'] >= start_timestamp]
    df = df[df['Close Time'] >= end_datetime]

    dataset_start = datetime.fromtimestamp(df['Open Time'][0]/1000)
    dataset_end = datetime.fromtimestamp(df['Close Time'][0] / 1000)
    print(dataset_start, dataset_end)

    # candle stick:
    # high / close < 0.1, close and high diff less then 10%
    df['high_low_diff_cri'] = (df['High'] / df['Low'] - 1) > 0.01
    # Close > Open
    df['price_up_cri'] = df['Close'] > df['Open']
    # Close / Low > 1.5%
    df['range_cri'] = (df['Close'] / df['Low'] - 1) > 0.015
    # volume > mean
    df['weekly_vol_mean'] = df['Volume'].rolling(7 * 12 * 24, min_periods=7 * 12 * 24).mean().shift(1)
    df['weekly_vol_mean_cri'] = df['Volume'] > df['weekly_vol_mean'] * 5

    # market cap > 100000
    df['market_cap_signal'] = df['Quote asset volume'] > 100000

    df['buy_signal'] = df['high_low_diff_cri'] & df['price_up_cri'] & df['range_cri'] & df['weekly_vol_mean_cri'] & df['market_cap_signal']

    df['up_trend'] = (df['Close'] / df['Open'] - 1) > 0.003
    df['sell_signal'] = [not any(window.to_list()) for window in df.up_trend.rolling(window=sell_window)]

    strategy_return = 1
    holding = False
    cost = 0
    trade_count = 0

    file = open(f"back_testing/{datetime.now().strftime('%m-%d-%H-%M-%S')}-{symbol}_result.txt", "w+")

    for index, row in df.iterrows():
        if(row['buy_signal'] is True and holding == False):
            result = f"buying {symbol} at {row['Close']}, {row['datetime']}"
            cost = row['Close']
            holding = True
            file.write(result + '\r\n')
            # print(result)

        if(row['sell_signal'] is True and holding == True):
            result = f"selling {symbol} at {row['Close']}, {row['datetime']}"
            strategy_return = strategy_return * row['Close'] / cost
            cost = 0
            holding = False
            file.write(result + '\r\n')
            trade_count += 1
            # print(result)

    result = f"{symbol} has return {str(strategy_return)} from {start} to {end}"
    file.write(result + '\r\n')
    file.close()
    print(result)
    print(trade_count)


if __name__ == '__main__':
    coins = get_symbols()

    for coin in coins:
        get_history(coin)

        print(f"{datetime.now().strftime('%H:%M:%S')} completed {coin}")