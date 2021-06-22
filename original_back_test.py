def back_testing(symbol):
    df = pd.read_csv(f"dataset/{symbol}.csv")
    # df = df[:30000]
    df['market_cap_signal'] = df['quote_asset_vol_mean'] > 100000
    df['buy_vol_signal'] = df['Volume'] > df['weekly_vol_mean'] * 5
    df['buy_price_signal'] = df['ptc'] > 0.0075
    df['rebound'] = [any(window.to_list()) for window in df.sell_signal.rolling(window=4)]
    df['buy_signal'] = df['buy_price_signal'] & df['buy_vol_signal'] & df['market_cap_signal'] & ~df['rebound']

    df['sell_vol_below_mean_signal'] = df['Volume'] < df['hourly_vol_mean'] * 0.5
    df['sell_vol_above_mean_signal'] = df['Volume'] > df['hourly_vol_mean'] * 5
    df['sell_vol_signal'] = df['sell_vol_below_mean_signal'] | df['sell_vol_above_mean_signal']
    df['sell_price_signal'] = df['ptc'] < -0.005

    df['absolute_sell_signal'] = df['ptc'] < -0.075
    df['sell_signal'] = ( df['sell_price_signal'] & df['sell_vol_signal'] ) | df['absolute_sell_signal']

    df.to_csv(f"back_testing/{symbol}-data.csv", index=False)

    strategy_return = 1
    holding = False
    cost = 0

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
            # print(result)


    result = f"{symbol} has return {str(strategy_return)} since 2020"
    file.write(result + '\r\n')
    file.close()
    print(result)

    return strategy_return