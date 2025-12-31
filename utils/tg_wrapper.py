import requests
import logging
import socket
import pandas as pd

from core.orchestrator import DataSourceConfig

# 設置 logger
logger = logging.getLogger(__name__)


class SendTGBot:
    # 類級別的標記，確保只設置一次 IPv4
    _ipv4_forced = False

    def __init__(self):
        # 強制使用 IPv4（只執行一次）
        if not SendTGBot._ipv4_forced:
            self._force_ipv4()
            SendTGBot._ipv4_forced = True

        # Load API config once
        tgbot_api = DataSourceConfig.load_tg_api_config()
        self.tg_token: str = tgbot_api.get('TOKEN')
        self.tg_group_id: str = tgbot_api.get('GROUP_ID')

        if self.tg_token is None or self.tg_group_id is None:
            logger.error('tgbot_api configuration not found.')
            raise ValueError('Telegram bot configuration is incomplete')

        logger.info(f'SendTGBot initialized with group_id: {self.tg_group_id}')


    def _force_ipv4(self):
        """強制所有 socket 連接使用 IPv4 以避免 IPv6 連接問題"""
        old_getaddrinfo = socket.getaddrinfo

        def new_getaddrinfo(*args, **kwargs):
            responses = old_getaddrinfo(*args, **kwargs)
            return [response for response in responses
                    if response[0] == socket.AF_INET]

        socket.getaddrinfo = new_getaddrinfo
        logger.info("Forced IPv4 for all network connections")


    def result_signal_df_to_txt(self, result_signal_df: pd.DataFrame) -> str:
        """將結果 DataFrame 轉換為文字訊息"""
        msg_df = result_signal_df.copy()
        msg_df['strat_name'] = msg_df['name'].str[:3] + '_' + msg_df['symbol']
        msg_df['date'] = msg_df['date'].dt.strftime('%y-%m-%d %H:%M')
        cols_to_drop = ['date_s1', 'saved_csv', 'name', 'symbol', 'signal', 'signal_s1']
        msg_df = msg_df.drop(columns=cols_to_drop)
        msg_df = msg_df[['date', 'strat_name', 'signal_plus']]
        msg_str = msg_df.to_string(index=False, justify='right')
        msg_str = 'result_signal_df:\n\n' + msg_str
        return msg_str


    def paradict_to_txt(self, status_str: str, pos_status: dict) -> str:
        """將參數字典轉換為文字訊息"""
        msg_line = []
        msg_dict = status_str + ':\n\n'
        for key, value in pos_status.items():
            msg_line.append(f'{key}: {value}')
        msg_dict += '\n'.join(msg_line)
        return msg_dict


    def send_df_msg(self, txt_msg: str, timeout: int = 10) -> bool:
        """
        發送訊息到 Telegram

        Args:
            txt_msg: 要發送的文字訊息
            timeout: 請求超時時間（秒）

        Returns:
            bool: 發送成功返回 True，失敗返回 False
        """
        try:
            # 使用 POST 方法和 JSON payload（更安全和可靠）
            url = f'https://api.telegram.org/bot{self.tg_token}/sendMessage'
            payload = {
                'chat_id': self.tg_group_id,
                'text': txt_msg,
                'parse_mode': 'HTML'  # 支持 HTML 格式
            }

            resp = requests.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()  # 如果狀態碼不是 2xx，拋出異常

            logger.info(f'Notification sent to Telegram successfully (status: {resp.status_code})')
            return True

        except requests.exceptions.Timeout:
            logger.error(f'Telegram request timed out after {timeout}s')
            return False

        except requests.exceptions.ConnectionError as e:
            logger.error(f'Connection error to Telegram API: {e}')
            return False

        except requests.exceptions.HTTPError as e:
            logger.error(f'HTTP error from Telegram API: {resp.status_code} - {resp.text}')
            return False

        except Exception as e:
            logger.error(f'Unexpected error sending Telegram message: {type(e).__name__} - {e}')
            return False