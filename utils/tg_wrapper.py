import requests
import logging
import gc
import pandas as pd

from core.orchestrator import DataSourceConfig


class SendTGBot:
    def __init__(self):
        # Load API config once
        tgbot_api = DataSourceConfig.load_tg_api_config()
        self.tg_token: str = tgbot_api.get('TOKEN')
        self.tg_group_id: str = tgbot_api.get('GROUP_ID')
        if self.tg_token is None or self.tg_group_id is None:
            logger.error('tgbot_api configuration not found.')


    def result_signal_df_to_txt(self, result_signal_df: pd.DataFrame):
        msg_df = result_signal_df.copy()
        msg_df['strat_name'] = msg_df['name'].str[:3] + '_' + msg_df['symbol']
        msg_df['date'] = msg_df['date'].dt.strftime('%y-%m-%d %H:%M')
        cols_to_drop = ['date_s1', 'saved_csv', 'name', 'symbol', 'signal', 'signal_s1']
        msg_df = msg_df.drop(columns=cols_to_drop)
        msg_df = msg_df[['date', 'strat_name', 'signal_plus']]
        msg_str = msg_df.to_string(index=False, justify='right')
        msg_str = 'result_signal_df:\n\n' + msg_str
        return msg_str


    def paradict_to_txt(self, status_str: str, pos_status: dict):
        msg_line = []
        msg_dict = status_str + ':\n\n'
        for key, value in pos_status.items():
            msg_line.append(f'{key}: {value}')
        msg_dict += '\n'.join(msg_line)
        return msg_dict


    def send_df_msg(self, txt_msg: str):
        msg = txt_msg
        tg_api_url = f'https://api.telegram.org/bot{self.tg_token}/sendMessage?chat_id={self.tg_group_id}&text={msg}'
        resp = requests.get(tg_api_url)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Your code, transformed to use logging
        if resp.status_code == 200:
            logging.info('notification has been sent to tg')
        else:
            logging.error('could not send msg')