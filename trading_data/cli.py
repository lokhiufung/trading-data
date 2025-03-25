import os
# from dotenv import load_dotenv
# load_dotenv('.env')

import click
from importlib import import_module
from datetime import datetime, timedelta

from trading_data.datalake_client import DatalakeClient

DL_CLIENT = DatalakeClient(datalake_dir=os.path.join(os.getenv('HOME'), '.trading-data'))
# Setting the date range (last month)
DEFAULT_END_DATE = datetime.today()


@click.group()
def cli():
    """
    CLI application to manage data.

    This application provides a set of commands for data management. You can perform
    operations like adding and updating data sources. For help on specific commands, 
    run them with the '-h' or '--help' flag.
    """
    pass


@cli.group()
def datalake():
    """
    Datalake operations group.

    This group includes commands related to Datalake operations. You can add new data sources
    or update existing ones. For specific operations, use 'add' or 'update' commands 
    with this group. For more information on these commands, use '-h' or '--help'.
    """
    pass


@datalake.command()
@click.option('--name', required=True, help='The name of the data source to update')
@click.option('--start-date', required=False, default=None, help='The starting date of new data. Default to be the date of trailing half year from the ending date')
@click.option('--end-date', required=False, default=None, help='The ending date of the new data. Default to be today')
def update(name, start_date, end_date):
    """
    Update an existing data source.

    This command allows you to update an existing data source specified by its name.
    """
    data_source = import_module(f'trading_data.data_sources.{name}_data_source')
    if end_date is None:
        end_date = datetime.today()
        # convert back to str
    if start_date is None:
        # use trailing 10 year as the default start_date
        start_date = end_date - timedelta(days=10*365)

    # convert back to str
    if not isinstance(start_date, str):
        start_date = datetime.strftime(start_date, "%Y-%m-%d")
    if not isinstance(end_date, str):
        end_date = datetime.strftime(end_date, "%Y-%m-%d")

    data_source.update_data(DL_CLIENT, start_date, end_date)


@datalake.command()
@click.option('--name', required=True, help='The name of the data source to add')
@click.option('--start-date', required=False, default=None, help='The starting date of new data. Default to be the date of trailing half year from the ending date')
@click.option('--end-date', required=False, default=None, help='The ending date of the new data. Default to be today')
def add(name, start_date, end_date):
    """
    Add a new data source.

    This command allows you to add a new data source with a specified name.
    """
    data_source = import_module(f'trading_data.data_sources.{name}_data_source')
    if end_date is None:
        end_date = datetime.today()
        # convert back to str
    if start_date is None:
        # use trailing 10 year as the default start_date
        start_date = end_date - timedelta(days=10*365)

    # convert back to str
    if not isinstance(start_date, str):
        start_date = datetime.strftime(start_date, "%Y-%m-%d")
    if not isinstance(end_date, str):
        end_date = datetime.strftime(end_date, "%Y-%m-%d")
    
    data_source.add_data(DL_CLIENT, start_date, end_date)


@datalake.command()
@click.option('--name', required=True, help='The name of the data source to add')
def delete(name):
    # prompt for confirmation
    click.confirm(f'Are you sure you want to delete data source={name}?', abort=True)
    DL_CLIENT.delete_data_source(name)


@datalake.command()
@click.option('--name', required=True, help='The name of the data source to migrate')
@click.option('--ver', required=True, help='The name of the ver to mirgrate')
def migrate(name, ver):
    DL_CLIENT.migrate_bar_data_to_timescaledb(
        data_source=name,
        ver_name=ver,
    )


@datalake.command()
@click.option('--name', required=True, help='The name of the data source to mirgrate')
@click.option('--ver', required=True, help='The name of the ver to mirgrate')
def info(name, ver):
    start_date, end_date = DL_CLIENT.get_current_time_period(name, ver_name=ver)
    data_menu = DL_CLIENT.get_data_menu(name)
    print(f'Info of data_source={name}:')
    print(f'{start_date=} {end_date=}')
    print('Data Menu')
    for asset_type in data_menu:
        n_assets = len(data_menu[asset_type])
        print(f'{asset_type=} {n_assets=}')
        

if __name__ == '__main__':
    cli()
