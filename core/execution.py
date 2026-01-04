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


class TelegramNotifier:
    """統一管理所有 Telegram 通知的類"""

    def __init__(self):
        self.tg = SendTGBot()
        self.queue = Queue()
        self.worker_thread = None
        self._start_worker()

    def _start_worker(self):
        """啟動後台工作線程"""
        if self.worker_thread is None or not self.worker_thread.is_alive():
            self.worker_thread = Thread(target=self._worker, daemon=True)
            self.worker_thread.start()
            logger.info('Telegram worker started')

    def _worker(self):
        """後台線程處理消息隊列"""
        while True:
            message_data = self.queue.get()

            if message_data is None:  # 停止信號
                logger.info('Telegram worker stopped')
                break

            txt_msg = message_data['message']
            context = message_data['context']

            # 簡單重試 2 次
            for attempt in range(1, 3):
                try:
                    success = self.tg.send_df_msg(txt_msg, timeout=20)

                    if success:
                        logger.info(f'✓ TG sent ({context})' + (f' [retry {attempt}]' if attempt > 1 else ''))
                        break
                    elif attempt < 2:
                        logger.warning(f'TG retry {attempt}/2 ({context})')
                        time.sleep(2)
                    else:
                        logger.warning(f'TG failed after 2 attempts ({context})')

                except Exception as e:
                    if attempt < 2:
                        logger.warning(f'TG error, retry {attempt}/2: {type(e).__name__}')
                        time.sleep(2)
                    else:
                        logger.error(f'TG error after 2 attempts ({context}): {e}')

            time.sleep(1)  # 避免限流
            self.queue.task_done()

    def send(self, txt_msg: str, context: str = ""):
        """異步發送消息（立即返回）"""
        self._start_worker()
        self.queue.put({'message': txt_msg, 'context': context})
        logger.info(f'TG queued ({context}), size: {self.queue.qsize()}')

    def wait(self, timeout: int = 60):
        """等待所有消息發送完成"""
        queue_size = self.queue.qsize()
        if queue_size == 0:
            logger.info('✓ No pending TG notifications')
            return True

        logger.info(f'Waiting for {queue_size} TG notifications (timeout: {timeout}s)...')
        try:
            self.queue.join()
            logger.info('All TG notifications sent')
            return True
        except Exception as e:
            logger.error(f'Error waiting for TG: {e}')
            return False

    def stop(self):
        """停止工作線程"""
        if self.worker_thread and self.worker_thread.is_alive():
            self.queue.put(None)
            self.worker_thread.join(timeout=5)
            logger.info('TG worker stopped')


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
        self.tg_notifier = TelegramNotifier()  # 統一的 TG 管理器
        return None

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
        market = self.markets.get(market_symbol)

        if market is None:
            logger.error('No matching market for %s', symbol)

        gc.collect()
        return market

    def get_pos_status(self, symbol: str):
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

        usdt_bal: float = float(balance['USDT']['total'])

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

        gc.collect()
        return pos_status

    def pos_adj(self):
        """倉位調整"""
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
            side = pos_status['side']

            if side == 'Sell':
                actual_pos = actual_pos * -1

            if actual_bid != actual_pos:
                corr = round((actual_pos - actual_bid), 5)
                adj = -1 * corr
                adj_value = abs(adj)

                if adj > 0:
                    logger.info(f'trade.long {adj_value}')
                    record_df = trade.trade_long(symbol, adj_value)
                elif adj < 0:
                    logger.info(f'trade.short {adj_value}')
                    record_df = trade.trade_short(symbol, adj_value)

                print(record_df)
                pos_status: dict = self.get_pos_status(symbol)
                txt_msg: str = tg.paradict_to_txt('pos_status (ADJ)', pos_status)
                self.tg_notifier.send(txt_msg, f"pos_adj - {symbol}")
            else:
                logger.info(f'{symbol} has no adjustment required')

    def prev_signal_df(self):
        signal_df = self.signal_df

        if os.path.exists(self.prev_signal_path):
            try:
                prev_signal_df = pd.read_csv(self.prev_signal_path)
            except Exception as e:
                logger.error(f'Failed to read {self.prev_signal_filename}: {e}')
                prev_signal_df = signal_df.copy()
                prev_signal_df['signal'] = 0
        else:
            prev_signal_df = signal_df.copy()
            prev_signal_df['signal'] = 0

        signal_df.to_csv(self.prev_signal_path, index=False)
        signal_df_s1 = prev_signal_df.copy().reset_index()
        signal_df_s1.rename(columns={'date': 'date_s1', 'signal': 'signal_s1'}, inplace=True)
        signal_df_s1 = signal_df_s1.drop(columns=['name', 'symbol', 'saved_csv'])

        gc.collect()
        return signal_df_s1

    def create_market_order(self):
        tg = SendTGBot()
        signal_df = self.signal_df
        trade = TradeRecord(self.signal_df)

        signal_df_s1 = self.prev_signal_df()

        result_signal_df = pd.concat([signal_df.reset_index(), signal_df_s1], axis=1)
        result_signal_df.drop(columns=['index', 'index'], inplace=True)
        result_signal_df = result_signal_df[['date', 'date_s1', 'name', 'symbol', 'saved_csv', 'signal', 'signal_s1']]
        result_signal_df['signal_plus'] = (result_signal_df['signal_s1'].astype(str) +
                                           result_signal_df['signal'].astype(str))

        print('===================== result_signal_df =====================')
        print(result_signal_df)

        txt_msg = tg.result_signal_df_to_txt(result_signal_df)
        self.tg_notifier.send(txt_msg, "result_signal_df")

        file_exists = os.path.isfile(self.signal_plus_path)
        result_signal_df.to_csv(
            self.signal_plus_path,
            mode='a',
            index=False,
            header=not file_exists
        )

        # mapping from signal_plus to human‑readable bucket
        signal_map = {
            '11': 'L/L', '10': 'L/0', '1-1': 'L/S',
            '01': '0/L', '00': '0/0', '0-1': '0/S',
            '-11': 'S/L', '-10': 'S/0', '-1-1': 'S/S'
        }

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
            .reindex(columns=cols, fill_value=0)
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

                # Trading signals
                if int(row['L/0']) > 0:
                    total_bet = int(row['L/0']) * bet_size
                    record_df = trade.trade_short(symbol, total_bet)
                    print(record_df)
                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '10')
                        ]
                    trade.trade_record_combine(after_signal_df, record_df)

                if int(row['L/S']) > 0:
                    total_bet = int(row['L/S']) * bet_size * 2
                    record_df = trade.trade_short(symbol, total_bet)
                    print(record_df)
                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '1-1')
                        ]
                    trade.trade_record_combine(after_signal_df, record_df)

                if int(row['0/L']) > 0:
                    total_bet = int(row['0/L']) * bet_size
                    record_df = trade.trade_long(symbol, total_bet)
                    print(record_df)
                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '01')
                        ]
                    trade.trade_record_combine(after_signal_df, record_df)

                if int(row['0/S']) > 0:
                    total_bet = int(row['0/S']) * bet_size
                    record_df = trade.trade_short(symbol, total_bet)
                    print(record_df)
                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '0-1')
                        ]
                    trade.trade_record_combine(after_signal_df, record_df)

                if int(row['S/L']) > 0:
                    total_bet = int(row['S/L']) * bet_size * 2
                    record_df = trade.trade_long(symbol, total_bet)
                    print(record_df)
                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '-11')
                        ]
                    trade.trade_record_combine(after_signal_df, record_df)

                if int(row['S/0']) > 0:
                    total_bet = int(row['S/0']) * bet_size
                    record_df = trade.trade_long(symbol, total_bet)
                    print(record_df)
                    after_signal_df = result_signal_df[
                        (result_signal_df['symbol'] == symbol) &
                        (result_signal_df['signal_plus'] == '-10')
                        ]
                    trade.trade_record_combine(after_signal_df, record_df)

                pos_status: dict = self.get_pos_status(symbol)
                time.sleep(5)
                txt_msg = tg.paradict_to_txt('pos_status (AFTER)', pos_status)
                self.tg_notifier.send(txt_msg, f"pos_status AFTER - {symbol}")

        # 檢查調整
        self.pos_adj()
        time.sleep(5)

        # 等待所有通知發送完成
        self.tg_notifier.wait(timeout=60)

        gc.collect()