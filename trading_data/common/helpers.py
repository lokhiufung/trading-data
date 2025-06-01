from trading_data.datalake_client import DatalakeClient

def if_update_data_menu(pdts, dl_client: DatalakeClient, data_source: str, asset_type: str = 'stock'):
    data_menu = dl_client.get_data_menu(data_source, flatten=False)
    if asset_type not in data_menu:
        data_menu[asset_type] = []
    current_pdts = data_menu[asset_type]  # TODO: we ONLY do updates for stock data now
    new_pdts = []
    for pdt in pdts:
        if pdt not in current_pdts:
            new_pdts.append(pdt)
    if len(new_pdts) > 0:
        data_menu[asset_type].extend(new_pdts)
        # write the data_menu into the datalake_dir
        dl_client.update_data_menu(data_source, data_menu)
    return new_pdts