import io
import requests
import pandas as pd
from tqdm import tqdm

from trading_data.datalake_client import DatalakeClient
from trading_data.common.date_ranges import get_dates
from trading_data.logger import get_logger


# Constants
DATA_SOURCE = 'binance'
BASE_URL = "https://data.binance.vision/"
PDT_MATCHING = {
    'BTC_USDT': 'BTCUSDT',
    'ETH_USDT': 'ETHUSDT',
}

logger = get_logger(__name__, logger_lv='info')


def get_path(trading_type, market_data_type, time_period, symbol, interval=None):
  trading_type_path = 'data/spot'
  if trading_type != 'spot':
    trading_type_path = f'data/futures/{trading_type}'
  if interval is not None:
    path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/'
  else:
    path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/'
  return path


def download_market_data(asset: str, date: str, trading_type='um') -> pd.DataFrame:
    """_summary_

    Args:
        asset (str): _description_
        date (str): _description_
        trading_type (str, optional): trading_type can be `um`, `cm` and `spot`. Defaults to 'um'.

    Returns:
        pd.DataFrame: _description_
    """
    # Construct the download URL for trade data
    path = get_path(trading_type, 'trades', time_period='daily', symbol=asset)
    url = f'{BASE_URL}{path}{asset}-trades-{date}.zip'
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    if response.status_code == 200:
        # Load the data into a DataFrame
        df = pd.read_csv(io.BytesIO(response.content), compression='zip')
        return df
    else:
        logger.error(f"Failed to retrieve data: Status code {response.status_code} {url=}")
        return pd.DataFrame()


def create_time_bars_from_tick_data(df: pd.DataFrame, freq: str='1min'):
    df = df.resample(freq).agg({
        'price': ['first', 'max', 'min', 'last'],
        'size': 'sum'
    })
    df.columns = ['_'.join(col).strip() for col in df.columns.values]
    df.rename(columns={
        'price_first': 'open',
        'price_max': 'high',
        'price_min': 'low',
        'price_last': 'close',
        'size_sum': 'volume'
    }, inplace=True)
    return df


def add_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    data_menu = {
        'perp': ['BTC_USDT', 'ETH_USDT'],
    }
    
    dl_client.add_data_source(DATA_SOURCE, data_menu)
    
    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            external_asset = PDT_MATCHING[asset]
            logger.info(f'Downloading {asset=} data from {DATA_SOURCE}...')
            for date in tqdm(pd.date_range(start=start_date, end=end_date)):
                try:
                    date_str = date.strftime('%Y-%m-%d')
                    df = download_market_data(external_asset, date_str)
                    # print(f'before {df=}')
                    df = df.rename(
                        columns={
                            'time': 'ts',
                            'qty': 'size'
                        }
                    )
                    # print(f'after {df=}')
                    df = df[['ts', 'price', 'size']]  # Adjust based on actual columns
                    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
                    df.set_index('ts', inplace=True)
                    df = create_time_bars_from_tick_data(df, freq='1min')
                    dl_client.add_data(DATA_SOURCE, asset_type, asset, data=df, ver_name='min_bar', date=date_str)
                except Exception as err:
                    logger.error(f'Error downloading {asset=} on {date_str}: {err}')


def update_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    data_menu = dl_client.get_data_menu(DATA_SOURCE)
    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            external_asset = PDT_MATCHING[asset]
            logger.info(f'Downloading {asset=} data from {DATA_SOURCE}...')
            for date in tqdm(pd.date_range(start=start_date, end=end_date)):
                try:
                    date_str = date.strftime('%Y-%m-%d')
                    df = download_market_data(external_asset, date_str)
                    df = df.rename(
                        columns={
                            'time': 'ts',
                            'qty': 'size'
                        }
                    )
                    df = df[['ts', 'price', 'size']]  # Adjust based on actual columns
                    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
                    df.set_index('ts', inplace=True)
                    df = create_time_bars_from_tick_data(df, freq='1min')
                    dl_client.update_data(DATA_SOURCE, asset_type, asset, data=df, ver_name='min_bar', date=date_str, how='replace')
                except Exception as err:
                    logger.info(f'Error downloading {asset=} on {date_str}: {err}')


if __name__ == '__main__':
    from trading_data.datalake_client import DatalakeClient
    dl_client = DatalakeClient()
    add_data(dl_client, '2023-01-01', '2023-01-31')
