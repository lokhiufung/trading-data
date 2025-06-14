import typing
import os
import time
from threading import Thread

import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData

from trading_data.datalake_client import DatalakeClient
from trading_data.common.helpers import if_update_data_menu
from trading_data.logger import get_logger
# from datetime import datetime

logger = get_logger("ib_data_source")


DATA_SOURCE = 'ib'
TWS_HOST = os.getenv("TWS_HOST", "127.0.0.1")
TWS_PORT = os.getenv("TWS_PORT", 4003)  # 7497 for paper trading, 7496 for real trading (default)
TWS_CLIENT_ID = os.getenv("TWS_CLIENT_ID", 1)


CONVERTOR = {
    'stock': 'STK',
    'futures': 'FUT',
}


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

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson):
        benign_errors = [2104, 2106, 2158]
        if errorCode in benign_errors:
            logger.info(f"Info {errorCode}: {errorString}")
        else:
            logger.error(f"Error {errorCode}: {errorString}")


from datetime import datetime

def get_most_recent_future_month():
    today = datetime.today()
    year = today.year
    month = today.month

    # CME futures contracts roll on a quarterly cycle: Mar, Jun, Sep, Dec
    quarterly_months = [3, 6, 9, 12]
    # Find the next quarterly month that is >= current month
    for m in quarterly_months:
        if month <= m:
            return f"{year}{m:02d}"
    # If none matched, we're past December, so roll to next year's March
    return f"{year+1}03"


def fetch_ib_data(symbol, exchange, currency, start_date, end_date, bar_size='1 Min', sec_type='STK'):
    app = IBApp()
    app.connect(TWS_HOST, int(TWS_PORT), clientId=int(TWS_CLIENT_ID))
    thread = Thread(target=app.run)
    thread.start()

    contract = Contract()
    if sec_type == 'FUT':
        contract.symbol = symbol
        contract.secType = "FUT"
        contract.exchange = 'CME'
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = get_most_recent_future_month()
    else:
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
    req_id_counter = 1000

    while current_start < end:
        current_end = min(current_start + max_chunk, end)
        duration = f"{(current_end - current_start).days} D"
        end_str = current_end.strftime("%Y%m%d-%H:%M:%S")

        app.data = []
        app.done = False
        app.reqHistoricalData(
            reqId=req_id_counter,
            contract=contract,
            endDateTime=end_str,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=0,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )
        req_id_counter += 1

        timeout = time.time() + 60
        while not app.done and time.time() < timeout:
            time.sleep(0.5)

        if app.data:
            df_chunk = pd.DataFrame(app.data, columns=["ts", "open", "high", "low", "close", "volume"])
            # Strip timezone suffix from IB timestamp strings and parse only date & time
            ts_clean = df_chunk["ts"].astype(str).str.extract(r'^(\d{8} \d{2}:\d{2}:\d{2})')[0]
            df_chunk["ts"] = pd.to_datetime(ts_clean, format="%Y%m%d %H:%M:%S")
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
        'stock': ['NVDA', 'AAPL', 'AMZN', 'GOOGL', 'TSLA', 'BAC', 'PLTR', 'MSFT', 'INTC', 'DUK', 'NEE', 'EIX', 'XOM', 'COP', 'VST'],
        # 'fx': ['EURUSD', 'USDJPY', 'GBPUSD', 'AUDUSD', 'USDCHF', 'USDCAD', 'NZDUSD'],
        'etf': ['DIA', 'SPY', 'QQQ', 'TQQQ', 'SQQQ']
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


def update_data(dl_client: DatalakeClient, start_date: str, end_date: str, pdts: typing.Optional[typing.List[str]]=None, asset_type: str='stock'):
    new_pdts = if_update_data_menu(pdts, dl_client, DATA_SOURCE, asset_type=asset_type)
    if len(new_pdts) > 0:
        logger.info(f'Added {new_pdts=} to the data menu under `{asset_type}`.')
    data_menu = dl_client.get_data_menu(DATA_SOURCE, flatten=False)
        
    for asset_type in data_menu:
        for asset in data_menu[asset_type]:
            if asset in pdts:
                # only update the specified pdts if any
                df = fetch_ib_data(asset, 'SMART', 'USD', start_date, end_date, sec_type=CONVERTOR[asset_type])
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
