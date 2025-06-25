import os

import pandas as pd
from tqdm import tqdm

from trading_data.datalake_client import DatalakeClient
from trading_data.logger import get_logger


logger = get_logger('dl_client', console_logger_lv='info', file_logger_lv='debug')


def list_files(folder_path, ext='csv'):
    # get all csv files in the datalake directory
    target_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(f'.{ext}'):
                target_files.append(os.path.join(root, file))
    return target_files


def main():
    dl_client = DatalakeClient()

    data_sources = dl_client.get_data_sources()
    for data_source in data_sources:
        data_menu = dl_client.get_data_menu(data_source, flatten=False)
        csv_files = list_files(
            folder_path=os.path.join(dl_client.datalake_dir, data_source),
            ext='csv',
        )
        logger.info(f"Processing data source: {data_source} with {len(csv_files)} CSV files.")
        for asset_type, assets in tqdm(data_menu.items()):
            for asset in assets:
                # add teh asset_type to the file name    
                # eg AAPL_2024-12-16_historical_data.csv to stock_AAPL_2024-12-16_historical_data.csv
                for csv_file in csv_files:
                    if f"{asset_type}_{asset}_" in csv_file or f"{asset_type}_{asset}_historical_data.csv" in csv_file:
                        df = pd.read_csv(csv_file)
                        df['ts'] = pd.to_datetime(df['ts'])
                        df.set_index('ts', inplace=True)
                        parquet_file = csv_file.replace('.csv', '.parquet')
                        try:
                            # df.to_parquet(parquet_file, index=True)
                            logger.info(f"✅ Converted to Parquet: {csv_file} → {parquet_file}")
                            os.remove(csv_file)
                        except:
                            logger.error(f"❌ Failed to convert: {csv_file}", exc_info=True)
    

main()