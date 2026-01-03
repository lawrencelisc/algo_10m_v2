import itertools

import pandas as pd
import datetime as dt

from pathlib import Path
from datetime import datetime
from loguru import logger

if __name__ == '__main__':

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    config_folder = Path(__file__).parent / 'config'
    signal_folder = Path(__file__).parent / 'data/Signal'

    add_filename = 'su_table_add.csv'
    su_filename = 'su_table.csv'
    prev_signal_filename = 'prev_signal_table.csv'

    add_config_path = config_folder / add_filename
    su_config_path = config_folder / su_filename
    signal_path = signal_folder / prev_signal_filename

    try:
        su_table_add_df = pd.read_csv(add_config_path)
        su_table_df = pd.read_csv(su_config_path)
        prev_signal_df = pd.read_csv(signal_path)
    except Exception as e:
        logger.error(f'Failed to read existing CSV {add_filename} / {su_filename}: {e}')
        su_table_add_df = pd.DataFrame()
        su_table_df = pd.DataFrame()

    for i, row in su_table_add_df.iterrows():
        if row['name'] not in su_table_df['name'].values:
            su_table_df.loc[len(su_table_df)] = row
        if row['name'] not in prev_signal_df['name'].values:
            saved_csv = row['name'] + '_' + row['endpt_col'] + '_' + row['symbol'] + '.csv'
            prev_signal_df.loc[len(prev_signal_df)] = {
                'date': prev_signal_df.iloc[-1]['date'],
                'name': row['name'],
                'symbol': row['symbol'],
                'saved_csv': saved_csv,
                'signal': '0'
            }

    try:
        su_table_df.to_csv(su_config_path, index=False)
        prev_signal_df.to_csv(signal_path, index=False)
        logger.info(f'Updated {su_filename}')
    except Exception as e:
        logger.error(f'Failed to update CSV: {e}')


