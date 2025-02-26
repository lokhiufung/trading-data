import io

from tqdm import tqdm
import pandas as pd
import requests

from trading_data.datalake_client import DatalakeClient
from trading_data.common.date_ranges import get_dates
from trading_data.logger import get_logger


logger = get_logger(__name__, logger_lv='info')


"""tick data sample
timestamp   symbol  side   size    price  tickDirection                            trdMatchID    grossValue  homeNotional  foreignNotional
0  1640995200  BTCUSDT  Sell  0.100  46200.5  ZeroMinusTick  61ca6489-4a19-506c-9ecd-a597d454eb87  4.620050e+11         0.100        4620.0500
1  1640995200  BTCUSDT  Sell  1.311  46200.5  ZeroMinusTick  78a666b9-211e-5f9c-9b0d-b47cd6d515e3  6.056886e+12         1.311       60568.8555
2  1640995200  BTCUSDT  Sell  0.516  46200.5  ZeroMinusTick  5dba0ee8-31a5-59d2-95a1-37957bbcdbe6  2.383946e+12         0.516       23839.4580
3  1640995200  BTCUSDT  Sell  2.500  46200.5  ZeroMinusTick  05d29d5f-5c6e-57e3-89a9-36ba55a5b65d  1.155012e+13         2.500      115501.2500
4  1640995200  BTCUSDT  Sell  0.271  46200.5  ZeroMinusTick  07cccda1-9c8e-5cc2-a6b2-5d7ec5d652d5  1.252034e+12         0.271       12520.3355
"""

DATA_SOURCE = 'bybit'
PDT_MATCHING = {
    # from internal to external
    'BTC_USDT': 'BTCUSDT',
    'ETH_USDT': 'ETHUSDT',
}


def create_time_bars_from_tick_data(df, freq='1min'):
    df = df.resample(freq).agg({
        'price': ['first', 'max', 'min', 'last'],
        'size': 'sum'
    })
    # Flatten the MultiIndex columns
    df.columns = ['_'.join(col).strip() for col in df.columns.values]

    # Rename columns to OHLCV
    df.rename(columns={
        'price_first': 'open',
        'price_max': 'high',
        'price_min': 'low',
        'price_last': 'close',
        'size_sum': 'volume'
    }, inplace=True)
    return df


def download_market_data(asset: str, date: str) -> pd.DataFrame:
    # Construct the download URL
    url = f'https://public.bybit.com/trading/{asset}/{asset}{date}.csv.gz'
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Decompress the content of the response
        # Read the content into a DataFrame
        df = pd.read_csv(io.BytesIO(response.content), header=0, compression='gzip')
        return df
    else:
        # If the request failed, print the status code
        print(f"Failed to retrieve data: Status code {response.status_code}")
        return pd.DataFrame()


def update_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    data_menu = dl_client.get_data_menu(DATA_SOURCE)
    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            external_asset = PDT_MATCHING[asset]
            logger.info(f'Downloaing {asset=} data from {DATA_SOURCE}...')
            for date in tqdm(get_dates(start_date, end_date)):
                try:
                    df = download_market_data(external_asset, date)
                    df = df[['timestamp', 'size', 'side', 'price']]  # drop other columns
                    df = df.rename(
                        columns={
                            'timestamp': 'ts',
                        }
                    )
                    df['ts'] = pd.to_datetime(df['ts'], unit='s')
                    df.set_index('ts', inplace=True)
                    # TODO: use only min bar now
                    df = create_time_bars_from_tick_data(df, freq='1min')
                    # REMINDER: no need to merge for bybit data
                    dl_client.update_data(
                        DATA_SOURCE,
                        asset_type,
                        asset,
                        data=df,
                        date=date,
                        ver_name='min_bar',
                        how='replace'
                    )
                except Exception as err:
                    logger.error(f'Error encountered when downloading {asset=} on {date}: {err}')


def add_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    # prepare data menu
    # TODO: hard-code the pairs fo the moment
    data_menu = {
        'perp': [
            'BTC_USDT',
            'ETH_USDT',
        ]
    }

    # add_data in datalake
    dl_client.add_data_source(DATA_SOURCE, data_menu)

    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            external_asset = PDT_MATCHING[asset]
            logger.info(f'Downloaing {asset=} data from {DATA_SOURCE}...')
            for date in tqdm(get_dates(start_date, end_date)):
                try:
                    df = download_market_data(external_asset, date)
                    df = df[['timestamp', 'size', 'side', 'price']]  # drop other columns
                    df = df.rename(
                        columns={
                            'timestamp': 'ts',
                        }
                    )
                    df['ts'] = pd.to_datetime(df['ts'], unit='s')
                    df.set_index('ts', inplace=True)
                    # TODO: use only min bar now
                    df = create_time_bars_from_tick_data(df, freq='1min')
                    # TODO: need to handle multi files
                    dl_client.add_data(
                        DATA_SOURCE,
                        asset_type,
                        asset,
                        data=df,
                        date=date,
                        ver_name='min_bar'
                    )
                except Exception as err:
                    logger.error(f'Error encountered when downloading {asset=} on {date}: {err}')


def get_data(self):
    pass



if __name__ == '__main__':
    from research.data.datalake_client import DatalakeClient

    dl_client = DatalakeClient()
    add_data()