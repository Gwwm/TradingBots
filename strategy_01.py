# 低成交、忽然爆上作為買入信號
import os

def strategy(df, symbol):
    df['weekly_vol_mean'] = df['Volume'].rolling(7 * 12 * 24, min_periods=7 * 12 * 24).mean().shift(1)
    df['daily_vol_mean'] = df['Volume'].rolling(12 * 24, min_periods=12*24).mean()
    df['vol_cri'] = df['daily_vol_mean'] > df['weekly_vol_mean'] * 100

    df['price_cri'] = df['Close'] > df['Open'] * 1.0075
    df['buy_signal'] = df['vol_cri'] & df['price_cri']

    df['sell_signal'] = df['Close'] < df['Open'] * 0.98

    df = df.drop(df[(df.buy_signal == False) & (df.sell_signal == False)].index)

    file_count = 0
    if os.path.isfile(f"back_test_data/{symbol}({file_count}).csv"):
       file_count += 1

    df.to_csv(f"back_test_data/{symbol}({file_count}).csv", index=False)

    return df