import pandas as pd


def strategy_02(df, coin):
    df['weekly_vol_mean'] = df['Volume'].rolling(7 * 12 * 24, min_periods=7 * 12 * 24).mean().shift(1)
    df['hourly_vol_mean'] = df['Volume'].rolling(12 * 24, min_periods=12 * 24).mean().shift(1)

    df['prev_price'] = df['Close'].shift(1)
    df['price_ptc'] = df['Close'] / df['prev_price'] - 1

    df['buy_vol_signal_1'] = df['Volume'] > df['weekly_vol_mean'] * 5
    df['buy_vol_signal_2'] = df['Volume'] > df['hourly_vol_mean'] * 2.5

    df['buy_price_signal_1'] = df['price_ptc'] > 0.02
    df['buy_price_signal_2'] = df['Close'] > df['Open'] * 1.02
    df['buy_price_signal_3'] = df['Close'] > df['Low'] * 1.03

    df['buy_signal'] = df['buy_vol_signal_1'] & df['buy_vol_signal_2'] & df['buy_price_signal_1'] & df[
        'buy_price_signal_2'] & df['buy_price_signal_3']

    # indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=3)
    # df['price_high_15min'] = df['Close'].rolling(window=indexer, min_periods=3).max().shift(-1)
    #
    # indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=12)
    # df['price_high_60min'] = df['Close'].rolling(window=indexer, min_periods=12).max().shift(-1)
    #
    # df['price_up_15mins'] = df['price_high_15min'] > df['Close']
    # df['price_up_60mins'] = df['price_high_60min'] > df['Close']

    # df['mean_21_period'] = df['Close'].rolling(21).mean().shift(1)
    # df['sd'] = df['Close'].rolling(21).std()
    # df['upper_bound'] = df['mean_21_period'] + 2 * df['sd']
    # df['lower_bound'] = df['mean_21_period'] - 2 * df['sd']
    #
    # df['price_signal'] = df['Close'] < df['lower_bound']
    # vol increase + price drop
    df['sell_vol_signal_1'] = df['Volume'] > df['hourly_vol_mean'] * 3
    df['sell_vol_signal_2'] = df['Volume'] > df['weekly_vol_mean'] * 8
    df['sell_price_signal_1'] = df['Close'] > df['High'] * 0.95
    df['sell_price_signal_2'] = df['Close'] > df['Open'] * 0.97

    df['sell_signal_1'] = df['sell_vol_signal_1'] & df['sell_vol_signal_2'] & df['sell_price_signal_1'] & df['sell_price_signal_2']

    # vol increase + price not increasing
    df['price_roll_6'] = df['Close'].rolling(6).mean()
    df['sell_price_signal_3'] = df['Close'] <= df['price_roll_6']

    df['sell_signal_2'] = df['sell_price_signal_3'] & df['sell_vol_signal_1'] & df['sell_vol_signal_2']

    # vol drop sharply
    df['vol_roll_6_lag'] = df['Volume'].rolling(6).mean().shift(1)
    df['sell_vol_signal_3'] = df['Volume'] < df['vol_roll_6_lag'] * 0.1

    df['sell_signal_3'] = df['sell_vol_signal_3'] & df['sell_price_signal_3']

    df['sell_signal'] = df['sell_signal_1'] | df['sell_signal_2'] | df['sell_signal_3']

    df = df.drop(df[(df.buy_signal == False) & (df.sell_signal == False)].index)
    df.to_csv(f"strategies/{coin}.csv", index=False)

    return df
