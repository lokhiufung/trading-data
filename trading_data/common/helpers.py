from trading_data.datalake_client import DatalakeClient

def if_update_data_menu(pdts, dl_client: DatalakeClient, data_source: str):
    data_menu = dl_client.get_data_menu(data_source, flatten=False)
    current_pdts = data_menu['stock']  # TODO: we ONLY do updates for stock data now
    new_pdts = []
    for pdt in pdts:
        if pdt not in current_pdts:
            new_pdts.append(pdt)
    if len(new_pdts) > 0:
        data_menu['stock'].extend(new_pdts)
        # write the data_menu into the datalake_dir
        dl_client.update_data_menu(data_source, data_menu)
    return new_pdts