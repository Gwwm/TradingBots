domain = 'https://api.coingecko.com/api/v3'
from main import pd, datetime

def get_exchanges():
    exchange_list_api = '/exchanges'

    df = pd.read_json(domain + exchange_list_api)
    df['total_vol'] = df.apply(lambda x: x['total_volumes'][1], axis=1)
    df['price'] = df.apply(lambda x: x['prices'][1], axis=1)
    df['datetime'] = df.apply(lambda x: datetime.fromtimestamp(x['prices'][0]/1000).strftime('%m/%d/%Y, %H:%M:%S'))
    df['total_vol_lag30'] = df['total_vol'].shift(30)