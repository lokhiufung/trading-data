import os

from tqdm import tqdm

from trading_data.datalake_client import DatalakeClient
from trading_data.logger import get_logger


logger = get_logger('dl_client', console_logger_lv='info', file_logger_lv='debug')


def list_csv_files(folder_path):
    # get all csv files in the datalake directory
    csv_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files


def main():
    dl_client = DatalakeClient()

    data_sources = dl_client.get_data_sources()
    for data_source in data_sources:
        data_menu = dl_client.get_data_menu(data_source, flatten=False)
        csv_files = list_csv_files(
            folder_path=os.path.join(dl_client.datalake_dir, data_source)
        )
        logger.info(f"Processing data source: {data_source} with {len(csv_files)} CSV files.")
        for asset_type, assets in tqdm(data_menu.items()):
            for asset in assets:
                # add teh asset_type to the file name    
                # eg AAPL_2024-12-16_historical_data.csv to stock_AAPL_2024-12-16_historical_data.csv
                for csv_file in csv_files:
                    if asset in csv_file and asset_type not in csv_file:
                        new_file_name = f"{asset_type}_{os.path.basename(csv_file)}"
                        new_file_path = os.path.join(os.path.dirname(csv_file), new_file_name)
                        try:
                            os.rename(csv_file, new_file_path)
                            logger.debug(f'Renamed {csv_file} to {new_file_path}')
                        except FileNotFoundError as e:
                            logger.warning(f"File not found error when renaming {csv_file} → {new_file_path}: {e}")
                        except Exception as e:
                            logger.error(f"Unexpected error when renaming {csv_file} → {new_file_path}: {e}")
    
    # after renaming, we do a sanity check
    for data_source in data_sources:
        data_menu = dl_client.get_data_menu(data_source, flatten=False)
        csv_files = list_csv_files(
            folder_path=os.path.join(dl_client.datalake_dir, data_source)
        )
        for asset_type, assets in tqdm(data_menu.items()):
            for asset in assets:
                # check if the asset_type is in the file name
                for csv_file in csv_files:
                    if asset in csv_file and asset_type not in csv_file:
                        logger.error(f"File {csv_file} does not contain the asset type {asset_type}.")
    

main()