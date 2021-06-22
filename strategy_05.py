# regression to identify the trend
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
from main import pd
import os

def strategy(df, symbol):
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
    rols = RollingOLS(y, x, window=window)
    rres = rols.fit()

    params = rres.params
    params.columns=[f"const_{window}", f"beta_{window}"]

    rsquared = rres.rsquared
    rsquared.name = f'r2_{window}'

    df = pd.concat([df, params, rsquared], axis=1)

    df['sell_significance'] = df[f'r2_{window}'] > 0.7
    df['sell_beta'] = df[f'beta_{window}'] > 0
    df['sell_signal'] = df['sell_beta'] & df['sell_significance']

    df = df.drop(df[(df.buy_signal == False) & (df.sell_signal == False)].index)

    file_count = 0
    if os.path.isfile(f"back_test_data/{symbol}({file_count}).csv"):
       file_count += 1

    df.to_csv(f"back_test_data/{symbol}({file_count}).csv", index=False)
    return df