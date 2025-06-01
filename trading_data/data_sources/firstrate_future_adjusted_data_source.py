import os

import pandas as pd
from tqdm import tqdm

# Import your data lake client and logger as necessary
from trading_data.datalake_client import DatalakeClient
from trading_data.logger import get_logger


DATA_SOURCE = 'firstrate_future_adjusted'
TICKER_LISTING_FILE_PATH = os.getenv('FIRSTRATE_FUTURE_TICKER_LISTING_FILE_PATH')
DATA_DIR = os.getenv('FIRSTRATE_FUTURE_ADJUSTED_DATA_DIR')
logger = get_logger(__name__, logger_lv='info')


def load_ticker_listing(file_path):
    df = pd.read_csv(file_path, header=0)
    df.columns = ['ticker', 'name', 'start_date', 'end_date']
    return df


def add_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    ticker_listing = load_ticker_listing(TICKER_LISTING_FILE_PATH)
    tickers = ticker_listing['ticker']
    # tickers = ['YM', 'NQ', 'ES']  # TEMP: hard-code for this moment

    data_menu = {
        'futures': list(tickers),
    }

    # add_data in datalake
    dl_client.add_data_source(DATA_SOURCE, data_menu)

    for asset_type in data_menu:
        for asset in tqdm(data_menu[asset_type]):
            logger.info(f'Extracting {asset=} firstrate future...')
            try:
                file_path = os.path.join(DATA_DIR, f'{asset}_1min_continuous_adjusted.txt')
                df = pd.read_csv(file_path, header=None, names=['ts', 'open', 'high', 'low', 'close', 'volume'])
                df['ts'] = pd.to_datetime(df['ts'])
                mask = (df['ts'] >= start_date) & (df['ts'] <= end_date)
                df = df.loc[mask]
                unique_dates = df['ts'].dt.date.unique()
                df.set_index('ts', inplace=True)
                # Split the dataframe by date and update the data for each date
                for unique_date in unique_dates:
                    date_str = unique_date.strftime('%Y-%m-%d')
                    df_date = df[df.index.date == unique_date]
                    # df_date = df_date[['open', 'high', 'low', 'close', 'volume']]
                    dl_client.add_data(
                        DATA_SOURCE,
                        asset_type,
                        asset,
                        data=df_date,
                        ver_name='min_bar',
                        date=date_str,
                    )
            except Exception as e:
                logger.error(f'Error encountered when extracting {asset=}: {e}')


def update_data(dl_client: DatalakeClient, start_date: str, end_date: str, pdts=None):
    data_menu = dl_client.get_data_menu(DATA_SOURCE)
    for asset_type in data_menu:
        for asset in tqdm(data_menu[asset_type]):
            logger.info(f'Extracting {asset=} data from firstrate future...')
            try:
                file_path = os.path.join(DATA_DIR, 'f{asset}_1min_continuous_adjusted.txt')
                df = pd.read_csv(file_path, header=None, names=['ts', 'open', 'high', 'low', 'close', 'volume'])
                df['ts'] = pd.to_datetime(df['ts'])
                df = df[(df['ts'] >= start_date) and (df['ts'] < end_date)]
                unique_dates = df['ts'].dt.date.unique()
                df.set_index('ts', inplace=True)
                for unique_date in unique_dates:
                    date_str = unique_date.strftime('%Y-%m-%d')
                    df_date = df[df.index.date == unique_date]
                    # df_date = df_date[['open', 'high', 'low', 'close', 'volume']]
                    dl_client.update_data(
                        DATA_SOURCE,
                        asset_type,
                        asset,
                        data=df_date,
                        ver_name='min_bar',
                        date=date_str,
                        how='replace'
                    )
            except Exception as e:
                logger.error(f'Error encountered when extracting {asset=}: {e}')

