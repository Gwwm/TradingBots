import pandas as pd
from main import get_symbols
from strategies.strategy_05 import strategy
import csv
import os
from datetime import datetime
from multiprocessing import Pool


def gen_return(symbol):
    strategy_code = '05'

    if os.path.isfile(f"dataset/{symbol}_5mins.csv"):
        df = pd.read_csv(f"dataset/{symbol}_5mins.csv")
        df = strategy(df, symbol)

    else:
        return 0

    if len(df) == 0:
        return 0

    strategy_return = 1
    holding = False
    cost = 0
    trade_count = 0

    file_count = 0

    while os.path.isfile(f"back_test_result/{symbol}-{strategy_code}({file_count}).csv"):
        file_count += 1

    file = open(f"back_test_result/{symbol}-{strategy_code}.csv", 'w', newline='')
    csvwriter = csv.writer(file)
    csvwriter.writerow(['Datetime', 'Direction', 'Price', 'Cost', 'Return'])

    for index, row in df.iterrows():
        if (row['buy_signal'] is True and holding == False):
            cost = row['Close']
            result = [row['datetime'], "Buy", row['Close']]
            csvwriter.writerow(result)

            holding = True
            # print(symbol, result)

        elif (row['sell_signal'] is True and holding == True):
            strategy_return = round(strategy_return * row['Close'] / cost * 0.999, 5)

            result = [row['datetime'], "Sell", row['Close'], round(row['Close'] / cost, 2), strategy_return]
            csvwriter.writerow(result)

            cost = 0
            holding = False
            trade_count += 1
            # print(symbol, result)

    result = f'{symbol} on ' \
             f'{(datetime.strptime(df.iloc[[-1]]["datetime"].item(), "%Y/%m/%d %H:%M:%S") - datetime.strptime(df.iloc[[0]]["datetime"].item(), "%Y/%m/%d %H:%M:%S")).days} days ' \
             f'has return {strategy_return}, with count {trade_count}'
    csvwriter.writerow([result])
    file.close()
    print(result)

    return strategy_return


if __name__ == '__main__':
    symbols = get_symbols()

    # for symbol in symbols:
    #     gen_return(symbol, '01')
    print(f"{datetime.now().strftime('%H:%M:%S')} start")

    results = []
    with Pool(4) as p:
        results = p.map(gen_return, symbols)

    print(f"{datetime.now().strftime('%H:%M:%S')} complete")
