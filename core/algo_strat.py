import gc

import pandas as pd
import numpy as np

from loguru import logger
from pathlib import Path

class AlgoStrategy:

    data_folder_GN = Path(__file__).parent.parent / 'data' / 'GrassNodeData'
    strat_folder = Path(__file__).parent.parent / 'data' / 'StratData'
    strat_folder.mkdir(parents=True, exist_ok=True)


    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df


    def data_collect(self):
        required_cols = {'name', 'symbol', 'endpt_col', 'strat'}
        missing = required_cols - set(self.strat_df.columns)
        if missing:
            logger.error(f'strat_df missing required columns: {missing}')
            return

        for _, row in self.strat_df.iterrows():
            name: str = str(row['name'])
            symbol: str = str(row['symbol'])
            endpt_col: str = str(row['endpt_col'])
            strat: str = str(row['strat'])

            filename: str = f'{name}_{symbol}.csv'
            filename_endpt: str = f'{name}_{endpt_col}_{symbol}.csv'
            file_path = self.data_folder_GN / filename
            file_path_endpt = self.strat_folder / filename_endpt

            try:
                existing_df = pd.read_csv(file_path, index_col=0)
                existing_df.index = pd.to_datetime(existing_df.index)
                if endpt_col != 'ohlc':
                    existing_df = existing_df[[endpt_col]]
                    existing_df = existing_df.dropna(subset=[endpt_col])
                existing_df.to_csv(file_path_endpt, date_format="%Y-%m-%d, %H:%M:%S")
                logger.info(f'File saved ({filename_endpt}) with {len(existing_df)} rows')
            except Exception as e:
                logger.error(f'Failed to read existing CSV {filename}: {e}')
                existing_df = pd.DataFrame()