# Buy Signal
# Bollinger Band as trend 突破
# Vol 突破

# Sell signal
# 20 ma as signal, break through = sell

def strategy(df, coin):
    df['weekly_vol_mean_lag'] = df['Volume'].rolling(7 * 12 * 24, min_periods=7 * 12 * 24).mean().shift(1)

    # Bollinger band
    df['ma_20'] = df['Close'].rolling(20, min_periods=20).mean()
    df['typical_price'] = (df['High'] + df['Low'] + df['Close']) / 3
    df['std'] = df['typical_price'].rolling(20, min_periods=20).std()
    df['tp_ma'] = df['typical_price'].rolling(20, min_periods=20).mean()
    df['upper_band'] = df['tp_ma'] + 2 * df['std']
    df['lower_band'] = df['tp_ma'] - 2 * df['std']

    # price signal
    df['buy_price_signal'] = df['Close'] > df['upper_band']

    # vol signal
    df['buy_vol_signal'] = df['Volume'] > df['weekly_vol_mean_lag'] * 5

    df['buy_signal'] = df['buy_price_signal'] & df['buy_vol_signal']

    # sell signal
    df['sell_signal'] = df['Close'] < df['lower_band']

    # df = df.drop(df[(df.buy_signal == False) & (df.sell_signal == False)].index)
    df.to_csv(f"strategies/{coin}.csv", index=False)

    return df