import time
import ccxt
import os
import gc
import sys
import requests
import pandas as pd
from loguru import logger

from threading import Thread
from queue import Queue

from pathlib import Path
from datetime import datetime, timezone
from ccxt.base.exchange import Exchange

from core.orchestrator import DataSourceConfig
from utils.trade_record import TradeRecord
from utils.tg_wrapper import SendTGBot


class SignalExecution:
    # constant
    strat_folder = Path(__file__).parent.parent / 'data' / 'StratData'
    signal_folder = Path(__file__).parent.parent / 'data' / 'Signal'
    prev_signal_filename = 'prev_signal_table.csv'
    signal_filename = 'signal_table.csv'
    signal_plus_filename = 'signal_table_plus.csv'
    prev_signal_path = signal_folder / prev_signal_filename
    signal_path = signal_folder / signal_filename
    signal_plus_path = signal_folder / signal_plus_filename

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    def __init__(self, signal_df: pd.DataFrame, bet_size: dict):
        self.signal_df = signal_df
        self.bet_size = bet_size
        # 創建消息隊列用於異步發送
        self.tg_queue = Queue()
        self.tg_worker_thread = None
        self._start_tg_worker()
        return None

    def _start_tg_worker(self):
        """啟動 Telegram 發送工作線程"""
        if self.tg_worker_thread is None or not self.tg_worker_thread.is_alive():
            self.tg_worker_thread = Thread(target=self._tg_worker, daemon=True)
            self.tg_worker_thread.start()
            logger.info('📤 Telegram worker thread started')

    def _tg_worker(self):
        """後台工作線程，處理 Telegram 消息隊列"""
        tg = SendTGBot()

        while True:
            try:
                # 從隊列獲取消息（阻塞等待）
                message_data = self.tg_queue.get()

                # 檢查是否為停止信號
                if message_data is None:
                    logger.info('🛑 Telegram worker received stop signal')
                    break

                txt_msg = message_data['message']
                context = message_data['context']

                # 直接發送消息（無重試、無分割）
                try:
                    success = tg.send_df_msg(txt_msg, timeout=15)

                    if success:
                        logger.info(f'✓ Telegram notification sent ({context})')
                    else:
                        logger.warning(f'✗ Telegram notification failed ({context})')

                except Exception as e:
                    logger.error(f'❌ Telegram send error ({context}): {type(e).__name__} - {str(e)[:200]}')

                # 標記任務完成
                self.tg_queue.task_done()

            except Exception as e:
                logger.error(f'❌ Telegram worker error: {type(e).__name__} - {str(e)}')

    def send_tg_notification(self, tg: SendTGBot, txt_msg: str, context: str = ""):
        """
        異步發送 Telegram 通知（立即返回，不阻塞）

        Args:
            tg: SendTGBot instance (保留此參數以兼容現有調用，但實際使用內部 tg)
            txt_msg: Message to send
            context: Description of what notification is for

        Returns:
            None (立即返回，不等待發送完成)
        """
        # 確保工作線程正在運行
        self._start_tg_worker()

        # 將消息加入隊列
        message_data = {
            'message': txt_msg,
            'context': context
        }
        self.tg_queue.put(message_data)
        logger.info(f'📬 Telegram message queued ({context}), queue size: {self.tg_queue.qsize()}')

    def wait_for_tg_notifications(self, timeout: int = 60):
        """
        等待所有 Telegram 通知發送完成

        Args:
            timeout: 最大等待時間（秒）

        Returns:
            bool: True if all sent, False if timeout
        """
        queue_size = self.tg_queue.qsize()
        if queue_size == 0:
            logger.info('✓ No pending Telegram notifications')
            return True

        logger.info(f'⏳ Waiting for {queue_size} Telegram notifications to complete (timeout: {timeout}s)...')

        try:
            # 等待隊列清空
            self.tg_queue.join()
            logger.info('✓ All Telegram notifications completed')
            return True

        except Exception as e:
            logger.error(f'❌ Error waiting for Telegram notifications: {e}')
            return False

    def stop_tg_worker(self):
        """停止 Telegram 工作線程"""
        if self.tg_worker_thread and self.tg_worker_thread.is_alive():
            # 發送停止信號
            self.tg_queue.put(None)
            # 等待線程結束
            self.tg_worker_thread.join(timeout=5)
            logger.info('🛑 Telegram worker thread stopped')

    # make position adjustment if find mismtach
    def pos_adj(self):
        tg = SendTGBot()
        trade = TradeRecord(self.signal_df)
        df = self.signal_df.copy()
        bid_df = self.bet_size.copy()
        df['signal'] = df['signal'].astype(int)

        for symbol in df['symbol'].unique():
            signal_sum = df.loc[df['symbol'] == symbol, 'signal'].sum()
            actual_bid = round(float(bid_df[symbol] * signal_sum), 5)
            pos_status = self.get_pos_status(symbol)
            actual_pos: float = pos_status['pos_size']
            # abs_actual_pos: float = actual_pos
            side = pos_status['side']

            if (side == 'Sell'): actual_pos = actual_pos * -1

            if (actual_bid != actual_pos):
                corr = round((actual_pos - actual_bid), 5)
                adj = -1 * corr
                adj_value = abs(adj)
                if (adj > 0):
                    print('>>>>>>>>>>>>>>>>>>>>> trade.long ', adj_value)
                    record_df = trade.trade_long(symbol, adj_value)
                if (adj < 0):
                    print('>>>>>>>>>>>>>>>>>>>>> trade.short ', adj_value)
                    record_df = trade.trade_short(symbol, adj_value)
                print(record_df)
                pos_status: dict = self.get_pos_status(symbol)
                status_str: str = 'pos_status (ADJ)'
                txt_msg: str = tg.paradict_to_txt(status_str, pos_status)
                self.send_tg_notification(tg, txt_msg, f"pos_adj - {symbol}")
            else:
                logger.info(f'{symbol} has no adjustment required')

    # get bybit api via ccxt
    def get_exchange_info(self, symbol: str):
        try:
            bybit_cfg = DataSourceConfig()
            bybit_api = bybit_cfg.load_bybit_api_config(symbol)
            self.bybit = ccxt.bybit({
                'apiKey': bybit_api[symbol + '_10M_API_KEY'],
                'secret': bybit_api[symbol + '_10M_SECRET_KEY'],
                'options': {'adjustForTimeDifference': True},
            })
            self.markets = self.bybit.load_markets()
        except Exception as e:
            logger.exception('Failed to load exchange info for %s: %s', symbol, e)
            raise
        market_symbol = f'{symbol}/USDT:USDT'
        try:
            market = self.markets[market_symbol]
            return market
        except KeyError:
            logger.error('No matching market for %s', symbol)
            return None
        gc.collect

    def get_pos_status(self, symbol: str):
        # initialization
        leverage: int = 1
        product_symbol = f'{symbol}USDT'

        market = self.get_exchange_info(symbol)
        position_info_dict: dict = self.bybit.fetch_positions(product_symbol)[0]['info']
        current_leverage = float(position_info_dict.get('leverage', 0))
        if current_leverage != leverage:
            try:
                self.bybit.set_leverage(leverage, product_symbol)
            except ccxt.BadRequest as exc:
                if 'leverage not modified' not in str(exc):
                    raise

        # print(position_info_dict)
        side: str = position_info_dict.get('side')
        pos_size: float = abs(float(position_info_dict.get('size')))
        markPrice: str = position_info_dict.get('markPrice')
        balance: float = self.bybit.fetch_balance()
        avg_price: float = position_info_dict.get('avgPrice')
        liq_price: float = position_info_dict.get('liqPrice')

        created_time_unix: float = position_info_dict.get('createdTime')
        created_time_s: int = int(created_time_unix) // 1000
        dt = datetime.utcfromtimestamp(created_time_s)
        created_time = dt.strftime('%y-%m-%d %H:%M')

        position_value: float = position_info_dict.get('positionValue')
        unrealised_pnl: float = position_info_dict.get('unrealisedPnl')
        cum_realised_pnl: float = position_info_dict.get('cumRealisedPnl')

        if balance is None:
            raise RuntimeError("fetch_balance() returned None")

        usdt_info = balance.get('USDT')
        if usdt_info is None:
            raise RuntimeError(f"'USDT' key missing in balance: {balance}")

        usdt_bal_raw = usdt_info.get('total')
        if usdt_bal_raw is None:
            raise RuntimeError(f"'total' field missing in USDT balance: {usdt_info}")

        usdt_bal: float = float(usdt_bal_raw)
        logger.info(f'Product symbol ({product_symbol}), '
                    f'current price (USDT): {markPrice}. '
                    f'account balance (USDT): {str(usdt_bal)}')

        time.sleep(0.05)
        pos_status = {
            'product_symbol': product_symbol,
            'leverage': leverage,
            'side': side,
            'pos_size': pos_size,
            'usdt_bal': usdt_bal,
            'markPrice': markPrice,
            'avg_price': avg_price,
            'liq_price': liq_price,
            'created_time': created_time,
            'position_value': position_value,
            'unrealised_pnl': unrealised_pnl,
            'cum_realised_pnl': cum_realised_pnl
        }
        gc.collect
        return pos_status

    def prev_signal_df(self):
        signal_df = self.signal_df
        file_exists = os.path.isfile(self.prev_signal_path)
        if os.path.exists(self.prev_signal_path):
            try:
                prev_signal_df = pd.read_csv(self.prev_signal_path)
            except pd.errors.EmptyDataError:
                prev_signal_df = pd.DataFrame()
                logger.error(f'Failed to read existing CSV {self.prev_signal_filename}')
            except Exception as e:
                prev_signal_df = signal_df.copy()
                prev_signal_df['signal'] = 0
                logger.error(f'Failed to read existing CSV {self.prev_signal_filename}: {e}')
        else:
            prev_signal_df = signal_df.copy()
            prev_signal_df['signal'] = 0

        signal_df.to_csv(self.prev_signal_path, index=False)
        signal_df_s1 = prev_signal_df.copy()
        signal_df_s1 = signal_df_s1.reset_index()

        signal_df_s1.rename(columns={'date': 'date_s1', 'signal': 'signal_s1'}, inplace=True)
        signal_df_s1 = signal_df_s1.drop(columns=['name', 'symbol', 'saved_csv'])
        gc.collect
        return signal_df_s1

    def create_market_order(self):
        tg = SendTGBot()
        signal_df = self.signal_df
        trade = TradeRecord(self.signal_df)

        signal_df_s1 = self.prev_signal_df()
        # trade = TradeRecord(self.signal_df)
        # hr_traded = trade.hr_traded()
        # print('hr_traded ?????????????', hr_traded)

        result_signal_df = pd.concat([signal_df.reset_index(), signal_df_s1], axis=1)
        result_signal_df.drop(columns=['index', 'index'], inplace=True)
        result_signal_df = result_signal_df[['date', 'date_s1', 'name', 'symbol', 'saved_csv', 'signal', 'signal_s1']]
        result_signal_df['signal_plus'] = (result_signal_df['signal_s1'].astype(str) +
                                           result_signal_df['signal'].astype(str))
        print('===================== result_signal_df =====================')
        print(result_signal_df)

        txt_msg = tg.result_signal_df_to_txt(result_signal_df)
        self.send_tg_notification(tg, txt_msg, "result_signal_df")

        file_exists = os.path.isfile(self.signal_plus_path)
        result_signal_df.to_csv(
            self.signal_plus_path,
            mode='a',
            index=False,
            header=not file_exists
        )

        # nowtime_str = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
        # unix_now = int(pd.Timestamp.utcnow().timestamp())
        # last_ts = result_signal_df['date'].iloc[-1]
        # unix_existing_ts = int(pd.to_datetime(last_ts)
        #                    .tz_localize('UTC')
        #                    .tz_convert('Asia/Hong_Kong')
        #                    .timestamp()
        #                    )
        # diff_ts_hrs = (unix_now - unix_existing_ts) > (75 * 60)

        # mapping from signal_plus to human‑readable bucket
        signal_map = {
            '11': 'L/L', '10': 'L/0', '1-1': 'L/S',
            '01': '0/L', '00': '0/0', '0-1': '0/S',
            '-11': 'S/L', '-10': 'S/0', '-1-1': 'S/S'
        }

        # the full, desired column order
        cols = ['L/L', 'S/L', '0/L', 'L/0', '0/0', 'S/0', '0/S', 'L/S', 'S/S']

        exec_list_df = (
            result_signal_df
            .assign(signal_bulk=lambda d: d['signal_plus'].map(signal_map))
            .assign(signal_bulk=lambda d: pd.Categorical(d['signal_bulk'], categories=cols, ordered=False))
            .pivot_table(
                index='symbol',
                columns='signal_bulk',
                values='signal',
                aggfunc='count',
                fill_value=0,
                observed=False
            )
            .reindex(columns=cols, fill_value=0)  # keep fixed order
            .rename_axis('index', axis=1)
            .reset_index()
        )
        print('===================== exec_list_df =====================')
        print(exec_list_df)

        trade = TradeRecord(self.signal_df)
        _10m_traded = trade._10m_traded()
        print('10 min excess? ', _10m_traded)

        if True:
            for _, row in exec_list_df.iterrows():
                symbol: str = row['symbol']
                total_bet: float = 0
                bet_size = float(self.bet_size.get(symbol, 0))

                # Trading signal >>>>>>>>>>>>> L/L
                if (int(row['L/L']) > 0):
                    print('L/L: ', row['L/L'])

                # Trading signal >>>>>>>>>>>>> L/0
                if (int(row['L/0']) > 0):
                    print('L/0: ', row['L/0'])
                    total_bet = int(row['L/0']) * bet_size
                    record_df = trade.trade_short(symbol, total_bet)
                    print(record_df)

                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '10')
                        ]

                    trade.trade_record_combine(after_signal_df, record_df)

                # Trading signal >>>>>>>>>>>>> L/S
                if (int(row['L/S']) > 0):
                    print('L/S: ', row['L/S'])
                    total_bet = int(row['L/S']) * bet_size * 2
                    record_df = trade.trade_short(symbol, total_bet)
                    print(record_df)

                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '1-1')
                        ]

                    trade.trade_record_combine(after_signal_df, record_df)

                # Trading signal >>>>>>>>>>>>> 0/L
                if (int(row['0/L']) > 0):
                    print('0/L: ', row['0/L'])
                    total_bet = int(row['0/L']) * bet_size
                    record_df = trade.trade_long(symbol, total_bet)
                    print(record_df)

                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '01')
                        ]

                    trade.trade_record_combine(after_signal_df, record_df)

                # Trading signal >>>>>>>>>>>>> 0/0
                if (int(row['0/0']) > 0):
                    print('0/0: ', row['0/0'])

                # Trading signal >>>>>>>>>>>>> 0/S
                if (int(row['0/S']) > 0):
                    print('0/S: ', row['0/S'])
                    total_bet = int(row['0/S']) * bet_size
                    record_df = trade.trade_short(symbol, total_bet)
                    print(record_df)

                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '0-1')
                        ]

                    trade.trade_record_combine(after_signal_df, record_df)

                # Trading signal >>>>>>>>>>>>> S/L
                if (int(row['S/L']) > 0):
                    print('S/L: ', row['S/L'])
                    total_bet = int(row['S/L']) * bet_size * 2
                    record_df = trade.trade_long(symbol, total_bet)
                    print(record_df)

                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '-11')
                        ]

                    trade.trade_record_combine(after_signal_df, record_df)

                # Trading signal >>>>>>>>>>>>> S/0
                if (int(row['S/0']) > 0):
                    print('S/0: ', row['S/0'])
                    total_bet = int(row['S/0']) * bet_size
                    record_df = trade.trade_long(symbol, total_bet)
                    print(record_df)

                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '-10')
                        ]
                    trade.trade_record_combine(after_signal_df, record_df)

                # Trading signal >>>>>>>>>>>>> S/S
                if (int(row['S/S']) > 0):
                    print('S/S: ', row['S/S'])

                pos_status: dict = self.get_pos_status(symbol)
                status_str: str = 'pos_status (AFTER)'
                txt_msg = tg.paradict_to_txt(status_str, pos_status)
                self.send_tg_notification(tg, txt_msg, f"pos_status AFTER - {symbol}")

        # check adjustment
        self.pos_adj()

        # 等待所有 Telegram 通知發送完成（可選）
        self.wait_for_tg_notifications(timeout=60)

        gc.collect