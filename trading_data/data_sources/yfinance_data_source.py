import os

import pandas as pd
import yfinance as yf

from trading_data.datalake_client import DatalakeClient
from trading_data.common.ticker_groups import download_sp500_list
from trading_data.logger import get_logger


DATA_SOURCE = 'yfinance'

logger = get_logger(__name__, logger_lv='info')


# Function to download historical data
def download_market_data(ticker, start_date, end_date):
    data = yf.download(ticker, start=start_date, end=end_date)
    # If columns are multi-index, drop the ticker level
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    # Select and reorder columns as in the old format
    data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
    # Rename columns to your desired lower-case names
    data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    return data


def update_data(dl_client: DatalakeClient, start_date, end_date, pdts=None):
    data_menu = dl_client.get_data_menu(DATA_SOURCE)
    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            if asset_type in ('stock', 'fx', 'etf'):
                if asset_type == 'fx':
                    external_asset = f'{asset}=X'
                else:
                    external_asset = asset
                df = download_market_data(external_asset, start_date, end_date)

                columns = ['open', 'high', 'low', 'close', 'volume']
                df.columns =columns
                df.index.name = 'ts'  # the `Date` is set as index
                # Convert 'ts' column to datetime and set as index
                df.index = pd.to_datetime(df.index)
                # df.set_index('ts', inplace=True)

                dl_client.update_data(
                    DATA_SOURCE,
                    asset_type,
                    asset,
                    data=df,
                    ver_name='day_bar',
                )
            else:
                raise ValueError(f'asset_type={asset_type} is not in the menu')


def add_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    # prepare data menu
    sp500_list = download_sp500_list()
    sp500_stocks = [company['ticker'] for company in sp500_list]

    data_menu = {
        'stock': sp500_stocks,
        'fx': ['EURUSD', 'USDJPY', 'GBPUSD', 'AUDUSD', 'USDCHF', 'USDCAD', 'NZDUSD'],
        'etf': ['DIA', 'SPY', 'QQQ']
    }

    # add_data in datalake
    dl_client.add_data_source(DATA_SOURCE, data_menu)

    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
                
            if asset_type in ('fx', 'stock', 'etf'):
                if asset_type == 'fx':
                    external_asset = f'{asset}=X'
                else:
                    external_asset = asset

                df = download_market_data(external_asset, start_date, end_date)
                columns = ['open', 'high', 'low', 'close', 'volume']
                df.columns =columns
                df.index.name = 'ts'  # the `Date` is set as index
                # Convert 'ts' column to datetime and set as index
                df.index = pd.to_datetime(df.index)
                # df.set_index('ts', inplace=True)

                dl_client.add_data(
                    DATA_SOURCE,
                    asset_type,
                    asset,
                    data=df,
                    ver_name='day_bar',
                )

            else:
                raise ValueError(f'asset_type={asset_type} is not in the menu')

