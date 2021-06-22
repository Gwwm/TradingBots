def strategy_03(df, coin):
    df['ma_13'] = df['Close'].rolling(13).mean()
    df['ma_288'] = df['Close'].rolling(288).mean()

    df['ma_99'] = df['Close'].rolling(99).mean()

    df['buy_signal'] = df['ma_13'] > df['ma_288']
    df['sell_signal'] = df['ma_99'] > df['ma_13']

    df['buy_signal'] = ~df['sell_signal'] & df['buy_signal']

    # df = df.drop(df[(df.buy_signal == False) & (df.sell_signal == False)].index)
    # df.to_csv(f"strategies/{coin}.csv", index=False)

    return df
