import yaml
import os
import gc
import pandas as pd

from pathlib import Path
from loguru import logger
from decimal import Decimal
from datetime import date, timedelta, datetime, tzinfo

class DataSourceConfig:


    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)


    def __init__(self):
        self.strat_df = None


    # Creat data folder and sub-folder at initial
    def create_folder(self):
        base_path = Path(__file__).parent.parent / 'data'
        folders = ['GrassNodeData', 'Signal', 'StratData', 'Trade']
        for folder_name in folders:
            folder_path = base_path / folder_name
            folder_path.mkdir(parents=True, exist_ok=True)
        gc.collect


    # Load su_table becoming strat_df
    def load_info_dict(self):
        try:
            project_root = os.path.dirname(os.path.dirname(__file__))
            csv_path = os.path.join(project_root, 'config', 'su_table.csv')
            self.strat_df = pd.read_csv(csv_path)
            strat_df = self.strat_df.copy()
        except FileNotFoundError:
            logger.error('su_table.csv not found.')
        gc.collect
        return strat_df


    # Load Grassnode API keys from config.yaml
    def load_gn_api_config():
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                gn_api = config.get('grassnode', {})
                required_keys = ['GN_API']
                for key in required_keys:
                    if key not in gn_api or not gn_api[key]:
                        raise Exception(f'Missing or empty {key} in config.yaml')
                return gn_api
        except FileNotFoundError:
            raise Exception('config.yaml not found.')
        except Exception as e:
            raise Exception(f'Error loading config.yaml: {e}')
        gc.collect


    # Load telegram tg token from config.yaml
    def load_tg_api_config():
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                tgbot_api = config.get('tg_bot', {})
                required_keys = ['TOKEN', 'GROUP_ID']
                for key in required_keys:
                    if key not in tgbot_api or not tgbot_api[key]:
                        raise Exception(f'Missing or empty {key} in config.yaml')
                return tgbot_api
        except FileNotFoundError:
            raise Exception('config.yaml not found.')
        except Exception as e:
            raise Exception(f'Error loading config.yaml: {e}')
        gc.collect


    # Load bybit API keys from config.yaml
    def load_bybit_api_config(self, symbol: str):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                bybit_sub = 'algo_10m_' + symbol.lower()
                bybit_sub_api = config.get(bybit_sub, {})
                sub_api = symbol + '_10M_API_KEY'
                sub_secret = symbol + '_10M_SECRET_KEY'
                required_keys = [sub_api, sub_secret]
                for key in required_keys:
                    if key not in bybit_sub_api or not bybit_sub_api[key]:
                        raise Exception(f'Missing or empty {key} in config.yaml')
                return bybit_sub_api
        except FileNotFoundError:
            raise Exception('config.yaml not found.')
        except Exception as e:
            raise Exception(f'Error loading config.yaml: {e}')
        gc.collect