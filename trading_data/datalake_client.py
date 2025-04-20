import re
import os
import typing

from tqdm import tqdm
from datetime import datetime
# from importlib import import_module
import yaml
import pandas as pd

from trading_data.types import DataVersionType
from trading_data.timescaledb import utils as timescaledb_utils
from trading_data.timescaledb.models import *


def extract_info_from_filename(filename, ver_name) -> tuple:
    if ver_name != 'day_bar':
        # Regular expression to match the pattern in the filename
        match = re.match(r"([\w.]+)_(\d{4}-\d{2}-\d{2})_historical_data.csv", filename)
        if match:
            product = match.group(1)
            date = match.group(2)
            return (product, date)
        else:
            return (None, None)
    else:
        # Regular expression to match the pattern in the filename
        match = re.match(r"([\w.]+)_historical_data.csv", filename)
        if match:
            product = match.group(1)
            return (product, )
    

def select_by_date_range(df, start_date, end_date):
    # Assuming 'df' is your DataFrame
    # First, convert the 'ts' column to datetime
    df['ts'] = pd.to_datetime(df['ts'])

    start_date = pd.to_datetime(start_date)
    # Convert the start and end dates to datetime
    end_date = pd.to_datetime(end_date)

    # Select the data within the date range
    filtered_df = df[(df['ts'] >= start_date) & (df['ts'] <= end_date)]
    return filtered_df


# data lake client object: for data selection, loading and transformation
class DatalakeClient:
    DEFAULT_DATALAKE_DIR = os.path.join(os.getenv('HOME'), '.trading-data')
    def __init__(self, datalake_dir=None):
        self.datalake_dir = datalake_dir
        if not self.datalake_dir:
            self.datalake_dir = self.DEFAULT_DATALAKE_DIR

        # create the datalake directory if not exists
        if not os.path.exists(self.datalake_dir):
            print(f'Creating datalake directory at {self.datalake_dir}')
            os.makedirs(self.datalake_dir)

        # self.template_file_path = template_file_path

        # with open(self.template_file_path, 'r') as f:
        #     self.template = yaml.safe_load(f)

    def get_current_time_period(self, data_source, ver_name):
        data_menu = self.get_data_menu(data_source)
        # get the first asset type and asset to extract current time period
        asset_type = list(data_menu.keys())[0]
        asset = data_menu[asset_type][0]

        ver_dir = os.path.join(self.datalake_dir, data_source, ver_name)
        if ver_name != 'day_bar':
            # extract the dates from file_names
            dates = [extract_info_from_filename(file_name, ver_name)[1] for file_name in os.listdir(ver_dir) if asset in file_name]
            # sort the dates and then return the starting date and ending date, dates are in string
            # extend my code here
            sorted_dates = sorted(dates, key=lambda d: datetime.strptime(d, "%Y-%m-%d"))  # Replace "%Y-%m-%d" with your date format
            return sorted_dates[0], sorted_dates[-1]  # Return the first and last date from the sorted list
        else:
            df = self.get_table(data_source, asset, 'day_bar')
            start_date = df.loc[0, 'ts']
            end_date = df.loc[len(df) - 1, 'ts']
            return start_date, end_date
    
    def get_data_sources(self):
        return os.listdir(self.datalake_dir)
    
    def get_data_menu(self, data_source, flatten=False):
        # load the data_menu first
        with open(os.path.join(self.datalake_dir, f'{data_source}_data_menu.yaml'), 'r') as f:
            data_menu = yaml.safe_load(f)
        if flatten:
            data_menu = [item for sublist in data_menu.values() for item in sublist]
        return data_menu
    
    def update_data_menu(self, data_source, data_menu):
        # load the data_menu first
        with open(os.path.join(self.datalake_dir, f'{data_source}_data_menu.yaml'), 'w') as f:
            yaml.safe_dump(data_menu, f)
        
    def get_index(self, data_source):
        index_file_path = os.path.join(self.datalake_dir, f'{data_source}/_index.csv')
        df_index = pd.read_csv(index_file_path, header=0)
        return df_index

    def list_indexes(self, data_source):
        index_file_path = os.path.join(self.datalake_dir, f'{data_source}/_index.csv')
        if os.path.exists(index_file_path):
            df_index = pd.read_csv(index_file_path, header=0)
            return list(df_index.columns)
        return []

    def _create_new_df_index(self, index_name, index_function, data_source):
        data_menu = self.get_data_menu(data_source)
        df_index = []
        # TODO: now hard coded `stock` and `fx`
        for ticker in tqdm(data_menu['stock'], desc='Iterating stocks'):
            df = self.get_table(data_source, ticker)
            index = index_function(df)
            df_index.append({'ticker': ticker, index_name: index})
        for ticker in tqdm(data_menu['stock'], desc='Iterating fx'):
            df = self.get_table(data_source, ticker)
            index = index_function(df)
            df_index.append({'ticker': ticker, index_name: index})
        df_index = pd.DataFrame(df_index)
        return df_index

    def create_index(self, data_source, index_name, index_function):
        index_file_path = os.path.join(self.datalake_dir, f'{data_source}/_index.csv')

        print(f'create a new index {index_name}')
        # create a new index
        df_index = self._create_new_df_index(index_name, index_function, data_source)
        if os.path.exists(index_file_path):
            print(f'updating the new index {index_name}')
            # load the old index first
            df_index_old = pd.read_csv(index_file_path, header=0)
            if index_name in df_index_old.columns:
                del df_index_old[index_name]  # replace the old index
            df_index = df_index_old.merge(df_index, on='ticker')

        df_index.to_csv(
            index_file_path,
            header=True,
            index=False
        )

    def delete_data_source(self, data_soure):
        # delete the data source
        data_menu_file = os.path.join(self.datalake_dir, f'{data_soure}_data_menu.yaml')
        os.remove(data_menu_file)
        # delete the data source directory
        data_source_dir = os.path.join(self.datalake_dir, data_soure)
        os.rmdir(data_source_dir)
        
    def add_data_source(self, data_source, data_menu):
        assert not data_source in self.get_data_sources()

        # register a new datalake for a new data_source and register a data_menu for it
        os.mkdir(os.path.join(self.datalake_dir, data_source))

        # write the data_menu into the datalake_dir
        with open(os.path.join(self.datalake_dir, f'{data_source}_data_menu.yaml'), 'w') as f:
            yaml.safe_dump(data_menu, f)

    def add_data(self, data_source: str, asset_type: str, asset: str, data: pd.DataFrame, ver_name, date=None):
        # assert data_source in self.get_data_sources()
        ver_dir = os.path.join(self.datalake_dir, data_source, ver_name)
        if not os.path.exists(ver_dir):
            os.mkdir(ver_dir)
        
        ver = self._convert_ver_name_to_ver(ver_name)
        file_path = self.get_file_path(data_source, asset, ver, date=date)

        assert not os.path.exists(file_path)  # ensure the adding data only at initialization, use update data later on

        # load the data_menu first
        with open(os.path.join(self.datalake_dir, f'{data_source}_data_menu.yaml'), 'r') as f:
            data_menu = yaml.safe_load(f)

        assert asset_type in data_menu
        assert asset in data_menu[asset_type]
        
        data.to_csv(
            file_path,
            index=True,
            header=True,
        )

    def update_data(self, data_source: str, asset_type: str, asset: str, data: pd.DataFrame, ver_name, date: str=None, how='merge'):
        assert data_source in self.get_data_sources()

        ver = self._convert_ver_name_to_ver(ver_name)

        # load the data_menu first
        with open(os.path.join(self.datalake_dir, f'{data_source}_data_menu.yaml'), 'r') as f:
            data_menu = yaml.safe_load(f)

        assert asset_type in data_menu
        assert asset in data_menu[asset_type]
        
        file_path = self.get_file_path(data_source, asset, ver, date=date)

        if how == 'merge':
            old_data = self.get_table(data_source, asset, set_index=True)
            self._merge_and_write_data(file_path, old_data, new_data=data)
        elif how == 'replace':
            data.to_csv(
                file_path,
                index=True,
                header=True,
            )
        else:
            raise ValueError(f'{how=} is not allowed.')
        
    @staticmethod
    def _merge_and_write_data(file_path:str, old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
        """
        Merges old_data and new_data DataFrames on the time dimension.
        In case of overlap, the rows from new_data are used.

        Parameters:
        old_data (pd.DataFrame): The old DataFrame.
        new_data (pd.DataFrame): The new DataFrame to merge.
        time_column (str): The name of the column representing time.

        Returns:
        pd.DataFrame: The merged DataFrame.
        """
        # Concatenate and sort by time
        merged_data = pd.concat([old_data, new_data]).sort_index()

        # Remove duplicate times, keeping the last (which comes from new_data)
        merged_data = merged_data[~merged_data.index.duplicated(keep='last')]

        # merged_data = merged_data.reset_index(drop=True)
        merged_data.to_csv(
            file_path,
            index=True,
            header=True,
        )

    def get_file_path(self, data_source, ticker, ver, date=None):
        if ver.value == DataVersionType.MIN_BAR.value:
            partition_name = 'min_bar'
        elif ver.value == DataVersionType.HOUR_BAR.value:
            partition_name = 'hour_bar'
        elif ver.value == DataVersionType.DAR_BAR.value:
            partition_name = 'day_bar'
        else:
            raise ValueError(f'{ver=} is not available now')
        
        if date is None:
            return os.path.join(self.datalake_dir, f'{data_source}/{partition_name}/{ticker}_historical_data.csv')
        else:
            return os.path.join(self.datalake_dir, f'{data_source}/{partition_name}/{ticker}_{date}_historical_data.csv')
    
    def get_table(
            self, 
            data_source, 
            ticker: str,
            ver_name: str='day_bar',
            start_date: str=None, 
            end_date: str=None,
            columns: typing.List[str]=None,
            set_index: bool=False,
            date: str=None,
        ) -> pd.DataFrame:

        ver = self._convert_ver_name_to_ver(ver_name)
        
        if date:
            # REMINDER: for miniute level data, files are seperated by dates
            file_path = self.get_file_path(data_source, ticker, ver=ver, date=date)
            df = pd.read_csv(file_path, header=0)
            if columns is not None and isinstance(columns, list):
                df = df[columns]
            if set_index:
                df['ts'] = pd.to_datetime(df['ts'])
                df.set_index('ts', inplace=True)
            return df
        else:
            # REMINDER: for daily data, files are seperated by assets only
            file_path = self.get_file_path(data_source, ticker, ver=ver)
            df = pd.read_csv(file_path, header=0)
            if start_date is not None or end_date is not None:
                df = select_by_date_range(df, start_date, end_date)
            if columns is not None and isinstance(columns, list):
                df = df[columns]
            if set_index:
                df['ts'] = pd.to_datetime(df['ts'])
                df.set_index('ts', inplace=True)
            return df

    def get_tables(
            self,
            data_source,
            tickers: typing.List[str],
            ver_name: str='day_bar',
            start_date: str=None,
            end_date: str=None,
            columns: typing.List[str]=None,
            set_index=False,
            dl_index=None,
        ) -> typing.Dict[str, pd.DataFrame]:
        assert ver_name == 'day_bar', f'You can only get multiple tables with {ver_name=}'
        tables = {}
        if dl_index is not None:
            df_index = self.get_index(data_source)
            # filter tickers by index
            tickers = [ticker for ticker in tickers if ticker in df_index[dl_index]]
        for ticker in tickers:
            df = self.get_table(data_source, ticker, ver_name, start_date, end_date, columns, set_index)
            tables[ticker] = df
        return tables
    
    @staticmethod
    def _convert_ver_name_to_ver(ver_name):
        # convert the ver_name to ver first
        if ver_name == 'day_bar':
            ver = DataVersionType.DAR_BAR
        elif ver_name == 'hour_bar':
            ver = DataVersionType.HOUR_BAR
        elif ver_name == 'min_bar':
            ver = DataVersionType.MIN_BAR
        else:
            raise ValueError(f'{ver_name=} is not supported')
        return ver
    
    def migrate_bar_data_to_timescaledb(self, data_source, ver_name):
        
        # migrating data from datalake to timescaledb by data type
        pdts = self.get_data_menu(data_source, flatten=True)
        ver_dir = os.path.abspath(os.path.join(self.datalake_dir, data_source, ver_name))

        db_client = timescaledb_utils.get_db_client()

        data_source_obj = db_client.query(DataSource).filter_by(name=data_source).first()
        # add the data_source if not exists
        if data_source_obj is None:
            data_source_obj = DataSource(name=data_source)
            db_client.add(data_source_obj)
            db_client.commit()
        
        # add pdt if not exists
        for pdt in pdts:
            product = db_client.query(Product).filter_by(name=pdt).first()
            if product is None:
                product = Product(name=pdt)
                db_client.add(product)
        db_client.commit()  # only commit once
        # print(f'{ver_name=}')
        for pdt in pdts:
            # if pdt not in ['ES', 'YM', 'NQ', 'MES']:
            #     continue
            file_paths = []
            for file_name in os.listdir(ver_dir):
                # print(f'{file_name}')
                info = extract_info_from_filename(file_name, ver_name)
                if info[0] == pdt:
                    file_paths.append(os.path.join(ver_dir, file_name))
            product = db_client.query(Product).filter_by(name=pdt).first()
            for file_path in tqdm(file_paths, desc=f'{pdt=}'):
                # _, date = extract_info_from_filename(file_path.split('/')[-1])
                # 1. load dataframe
                df = pd.read_csv(file_path, header=0)
                bars = df[['ts', 'open', 'high', 'low', 'close', 'volume']].to_dict(orient='records')
                # add product id and data source id to the bars
                bars = [{'product_id': product.id, 'data_source_id': data_source_obj.id, 'bar_type': ver_name, **bar} for bar in bars]
                db_client.bulk_insert_mappings(OhlcvBar, bars)  # bulk insert for better performance

        db_client.commit()
        db_client.close()





