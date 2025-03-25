import os
import time
from threading import Thread

import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData

from trading_data.datalake_client import DatalakeClient
from trading_data.logger import get_logger
# from datetime import datetime

logger = get_logger("ib_data_source")


DATA_SOURCE = 'ib'
TWS_HOST = os.getenv("TWS_HOST", "127.0.0.1")
TWS_PORT = os.getenv("TWS_PORT", 4003)  # 7497 for paper trading, 7496 for real trading (default)
TWS_CLIENT_ID = os.getenv("TWS_CLIENT_ID", 1)


class IBApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = []
        self.connected = False

    def historicalData(self, reqId, bar: BarData):
        print(bar)
        self.data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    def historicalDataEnd(self, reqId, start, end):
        self.done = True

    def error(self, reqId, errorCode, errorString):
        benign_errors = [2104, 2106, 2158]
        if errorCode in benign_errors:
            logger.info(f"Info {errorCode}: {errorString}")
        else:
            logger.error(f"Error {errorCode}: {errorString}")

def fetch_ib_data(symbol, exchange, currency, start_date, end_date, bar_size='1 Min'):
    app = IBApp()
    app.connect(TWS_HOST, int(TWS_PORT), clientId=int(TWS_CLIENT_ID))
    thread = Thread(target=app.run)
    thread.start()

    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = exchange
    contract.currency = currency

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    app.data = []
    app.done = False

    dfs = []
    current_start = start
    max_chunk = pd.Timedelta(days=30)

    while current_start < end:
        current_end = min(current_start + max_chunk, end)
        duration = f"{(current_end - current_start).days} D"
        end_str = current_end.strftime("%Y%m%d-%H:%M:%S")

        app.data = []
        app.done = False
        app.reqHistoricalData(
            reqId=1,
            contract=contract,
            endDateTime=end_str,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )

        timeout = time.time() + 60
        while not app.done and time.time() < timeout:
            time.sleep(0.5)

        if app.data:
            df_chunk = pd.DataFrame(app.data, columns=["ts", "open", "high", "low", "close", "volume"])
            df_chunk["ts"] = pd.to_datetime(df_chunk["ts"])
            df_chunk.set_index("ts", inplace=True)
            dfs.append(df_chunk)

        current_start = current_end

    app.disconnect()
    thread.join()

    if dfs:
        return pd.concat(dfs).sort_index()
    else:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


def add_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    # Prepare data men
    data_menu = {
        'stock': ['NVDA', 'AAPL', 'AMZN', 'GOOGL', 'TSLA', 'BAC', 'PLTR', 'MSFT', 'INTC'],
        # 'fx': ['EURUSD', 'USDJPY', 'GBPUSD', 'AUDUSD', 'USDCHF', 'USDCAD', 'NZDUSD'],
        # 'etf': ['DIA', 'SPY', 'QQQ']
    }

    # Register data source
    dl_client.add_data_source(DATA_SOURCE, data_menu)

    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            if asset_type == 'fx':
                symbol = asset  # may require mapping for IB contract format
            else:
                symbol = asset

            try:
                df = fetch_ib_data(symbol, 'SMART', 'USD', start_date, end_date)
                if not df.empty:
                    df['date'] = df.index.date
                    for day, df_day in df.groupby('date'):
                        df_day = df_day.drop(columns=['date'])
                        dl_client.add_data(
                            DATA_SOURCE,
                            asset_type,
                            asset,
                            data=df_day,
                            ver_name='min_bar',
                            date=day.strftime('%Y-%m-%d'),
                        )
            except Exception as e:
                logger.error(f"Failed to fetch or upload data for {asset_type}/{asset}: {e}")


def update_data(dl_client: DatalakeClient, start_date: str, end_date: str):
    data_menu = dl_client.get_data_menu(DATA_SOURCE, flatten=False)

    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            df = fetch_ib_data(asset, 'SMART', 'USD', start_date, end_date)
            if not df.empty:
                df['date'] = df.index.date
                for day, df_day in df.groupby('date'):
                    df_day = df_day.drop(columns=['date'])
                    dl_client.update_data(
                        DATA_SOURCE,
                        asset_type,
                        asset,
                        data=df_day,
                        ver_name='min_bar',
                        date=day.strftime('%Y-%m-%d'),
                        how='replace',
                    )
