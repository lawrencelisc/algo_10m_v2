import os
import time
import ccxt
import gc
import csv
import sys

import pandas as pd
import numpy as np

from pathlib import Path
from loguru import logger
from datetime import datetime, timezone
from ccxt.base.exchange import Exchange
from core.orchestrator import DataSourceConfig


class TradeRecord:

    def __init__(self, signal_df: pd.DataFrame):
        self.signal_df = signal_df
        trade_folder = Path(__file__).parent.parent / 'data' / 'Trade'
        trade_folder.mkdir(parents=True, exist_ok=True)

        trade_filename: str = 'bybit_trade_record.csv'
        trade_hist_filename: str = 'bybit_trade_hist.csv'
        after_signal_filename: str = 'after_signal_df.csv'
        return_filename: str = 'return_df.csv'

        self.file_path_trade = trade_folder / trade_filename
        self.file_path_trade_hist = trade_folder / trade_hist_filename
        self.file_path_si = trade_folder / after_signal_filename
        self.file_path_re = trade_folder / return_filename

        signal_folder = Path(__file__).parent.parent / 'data' / 'Signal'
        prev_signal_filename = 'prev_signal_table.csv'
        signal_filename = 'signal_table.csv'
        self.prev_signal_path = signal_folder / prev_signal_filename
        self.signal_path = signal_folder / signal_filename

        self.file_path_trade.touch(exist_ok=True)
        self.file_path_trade_hist.touch(exist_ok=True)
        self.file_path_si.touch(exist_ok=True)
        self.file_path_re.touch(exist_ok=True)

        self.col_order = [
            'timestamp',
            'datetime',
            'symbol',
            'order',
            'type',
            'side',
            'takerOrMaker',
            'price',
            'amount',
            'cost',
            'info.symbol',
            'info.orderType',
            'info.underlyingPrice',
            'info.orderLinkId',
            'info.orderId',
            'info.stopOrderType',
            'info.execTime',
            'info.feeCurrency',
            'info.createType',
            'info.execFeeV2',
            'info.feeRate',
            'info.tradeIv',
            'info.blockTradeId',
            'info.markPrice',
            'info.execPrice',
            'info.markIv',
            'info.orderQty',
            'info.orderPrice',
            'info.execValue',
            'info.closedSize',
            'info.execType',
            'info.seq',
            'info.side',
            'info.indexPrice',
            'info.leavesQty',
            'info.isMaker',
            'info.execFee',
            'info.execId',
            'info.marketUnit',
            'info.execQty',
            'info.extraFees',
            'info.nextPageCursor',
            'fee.currency',
            'fee.cost',
            'fee.rate',
            'fees.currency',
            'fees.cost',
            'fees.rate'
        ]


    def get_exchange_trade(self, symbol: str):
        market_symbol = f'{symbol}/USDT:USDT'
        try:
            bybit_cfg = DataSourceConfig()
            bybit_api = bybit_cfg.load_bybit_api_config(symbol)
            self.bybit = ccxt.bybit({
                'apiKey': bybit_api[symbol + '_10M_API_KEY'],
                'secret': bybit_api[symbol + '_10M_SECRET_KEY'],
                'enableRateLimit': True,
                'options': {'default': 'swap'},
            })
            self.markets = self.bybit.load_markets()
        except Exception as e:
            logger.exception("Failed to load exchange info for %s: %s", symbol, e)
            raise
        try:
            market = self.markets[market_symbol]
            return market
        except KeyError:
            logger.error("No matching market for %s", symbol)
            return None


    def _10m_traded(self):
        unix_now_ts: float = float(datetime.now(timezone.utc).timestamp())
        if os.path.exists(self.prev_signal_path):
            try:
                df = pd.read_csv(self.prev_signal_path)
            except pd.errors.EmptyDataError as e:
                df = pd.DataFrame()
                logger.error(f'Failed to read existing CSV {self.prev_signal_path}: {e}')
                return False
            except Exception as e:
                logger.error(f'Error reading {self.prev_signal_path}: {e}')
                return False
        else:
            df = pd.DataFrame()

        if df.empty:
            return False

        prev_dates = sorted(df['date'].unique())
        prev_last_date = prev_dates[-1]
        unix_prev_last_ts: float = float((pd.to_datetime(prev_last_date)).timestamp())
        unix_diff_ts: float = (unix_now_ts - unix_prev_last_ts)

        return (unix_diff_ts > 10 * 60)


    def trade_record_combine(self, after_signal_df: pd.DataFrame, record_df: pd.DataFrame):
        def is_file_empty(path):
            return os.path.getsize(path) == 0


        file_si_empty = is_file_empty(self.file_path_si)
        after_signal_df.to_csv(
            self.file_path_si,
            mode='a',
            index=False,
            header=file_si_empty
        )

        file_re_empty = is_file_empty(self.file_path_re)
        record_df.to_csv(
            self.file_path_re,
            mode='a',
            index=False,
            header=file_si_empty
        )
        strat_hist_df = after_signal_df.copy()

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        if (len(record_df) > 1):
            out = {}
            for col in record_df.columns:
                col_val = record_df[col]
                col_num = pd.to_numeric(col_val, errors='coerce')

                is_numeric = pd.api.types.is_numeric_dtype(col_val) or not col_num.isna().all()
                first_val = col_val.iloc[0] if len(col_val) else np.nan
                all_same = col_val.eq(first_val).all()

                if all_same:
                    out[col] = first_val
                elif is_numeric:
                    out[col] = col_val.sum(skipna=True)
                else:
                    out[col] = first_val

            rec_df = pd.DataFrame([out])
            num_cols = rec_df.select_dtypes(include=[np.number]).columns
            rec_df[num_cols] = rec_df[num_cols].replace(0, np.nan)
        else:
            rec_df = record_df.iloc[[0]].copy()
        print(rec_df)
        rec_dict: dict = rec_df.to_dict('records')[0]
        row_count: int = len(strat_hist_df)

        strat_hist_df['order_id'] = rec_dict['order']
        strat_hist_df['t_timestamp'] = rec_dict['timestamp']
        strat_hist_df['t_datetime'] = rec_dict['datetime']
        strat_hist_df['t_type'] = rec_dict['type']
        strat_hist_df['side'] = rec_dict['side']
        strat_hist_df['takerOrMaker'] = rec_dict['takerOrMaker']
        strat_hist_df['price'] = rec_dict['price']
        strat_hist_df['amount'] = float(rec_dict['amount']) / row_count
        strat_hist_df['cost'] = float(rec_dict['cost']) / row_count
        strat_hist_df['product_symbol'] = rec_dict['info.symbol']
        strat_hist_df['feeCurrency'] = rec_dict['fee.currency']
        strat_hist_df['fee.cost'] = float(rec_dict['fee.cost']) / row_count
        strat_hist_df['fee.rate'] = rec_dict['fee.rate']
        strat_hist_df.drop('date_s1', axis=1, inplace=True)
        strat_hist_df.drop('signal_s1', axis=1, inplace=True)
        strat_hist_df.drop('signal_plus', axis=1, inplace=True)

        print(strat_hist_df)
        write_header = not os.path.exists(self.file_path_trade_hist) or \
                       os.path.getsize(self.file_path_trade_hist) == 0
        strat_hist_df.to_csv(self.file_path_trade_hist, mode='a', header=write_header, index=False)


    def record_to_df(self, trade_param_list: any):
        row = []
        for t_element in trade_param_list:
            base = {t_name: t_data for t_name, t_data in t_element.items() if t_name not in ("info", "fee", "fees")}
            info = {f"info.{t_name}": t_data for t_name, t_data in (t_element.get("info") or {}).items()}
            single_fee = {f"fee.{t_name}": t_data for t_name, t_data in (t_element.get("fee") or {}).items()}

            fees_list = t_element.get("fees") or [{}]  # ensure at least one row
            for f in fees_list:
                feeitem = {f"fees.{t_name}": f.get(t_name) for t_name in ("currency", "cost", "rate")}
                row.append({**base, **info, **single_fee, **feeitem})
        return row

    def trade_long(self, symbol: str, total_bet: float):

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        print('symbol: ', symbol)
        print('total_bet: ', total_bet)
        category: str = 'linear'
        mode: int = 0                                               # positionIdx mode setting
        leverage: int = 1
        market_symbol = f'{symbol}/USDT:USDT'
        product_symbol = f'{symbol}USDT'
        params = {
            'category': 'linear',
            'positionIdx': 0,                                       # 1=long, 2=short, 0=one-way
            'timeInForce': 'IOC',
        }
        market = self.get_exchange_trade(symbol)
        print(market)
        print('---------------------------------------------------------', symbol)

        position_info_dict: dict = self.bybit.fetch_positions(market_symbol)[0]['info']
        print(position_info_dict)
        current_leverage = float(position_info_dict.get('leverage', 0))
        if current_leverage != leverage:
            try:
                self.bybit.set_leverage(leverage, product_symbol)
            except ccxt.BadRequest as exc:
                if 'leverage not modified' in str(exc).lower():
                    logger.debug('Leverage not modified (benign): %s', exc)
                else:
                    logger.exception('Unexpected BadRequest from exchange')
                    raise
        current_pos = int(position_info_dict.get('positionIdx', 0))

        try:
            order = self.bybit.create_order(
                symbol = product_symbol,
                type = 'market',
                side = 'buy',
                amount = total_bet,
                price = None,
                params = params
            )
            time.sleep(0.2)
            logger.info(f"Long opened, oderId: {order.get('id', order)}")
        except ccxt.BaseError as e:
            logger.exception("Create order failed")                     # prints traceback and message
            if hasattr(e, "body"): logger.error("Exchange body: %s", e.body)
            if hasattr(e, "headers"): logger.error("Exchange headers: %s", e.headers)
            return False

        print(order)
        order_id = order.get('id', order)
        trade_param_list = self.bybit.fetch_order_trades(order_id, product_symbol)
        print(trade_param_list)

        record_list = self.record_to_df(trade_param_list)
        record_df = pd.DataFrame(record_list)
        record_df = record_df.drop(columns=record_df.columns[0])
        record_df['rec_time'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        record_df.set_index('rec_time', inplace=True)
        record_df = record_df.reindex(columns=self.col_order)
        write_header = (not os.path.exists(self.file_path_trade)) or (os.path.getsize(self.file_path_trade) == 0)
        record_df.to_csv(
            self.file_path_trade,
            mode='a',
            index=False,
            header=write_header
        )

        print('---------------------------------------------------------', symbol)

        return record_df

    def trade_short(self, symbol: str, total_bet: float):

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        print('symbol: ', symbol)
        print('total_bet: ', total_bet)
        category: str = 'linear'
        mode: int = 0                                               # positionIdx mode setting
        leverage: int = 1
        market_symbol = f'{symbol}/USDT:USDT'
        product_symbol = f'{symbol}USDT'
        params = {
            'category': 'linear',
            'positionIdx': 0,                                       # 1=long, 2=short, 0=one-way
            'timeInForce': 'IOC',
        }
        market = self.get_exchange_trade(symbol)
        print(market)

        print('---------------------------------------------------------', symbol)

        position_info_dict: dict = self.bybit.fetch_positions(market_symbol)[0]['info']
        print(position_info_dict)
        current_leverage = float(position_info_dict.get('leverage', 0))
        if current_leverage != leverage:
            try:
                self.bybit.set_leverage(leverage, product_symbol)
            except ccxt.BadRequest as exc:
                if 'leverage not modified' in str(exc).lower():
                    logger.debug('Leverage not modified (benign): %s', exc)
                else:
                    logger.exception('Unexpected BadRequest from exchange')
                    raise
        current_pos = int(position_info_dict.get('positionIdx', 0))
        try:
            order = self.bybit.create_order(
                symbol = product_symbol,
                type = 'market',
                side = 'sell',
                amount = total_bet,
                price = None,
                params = params
            )
            time.sleep(0.2)
            logger.info(f"Short opened, oderId: {order.get('id', order)}")
        except ccxt.BaseError as e:
            logger.exception("Create order failed")                     # prints traceback and message
            if hasattr(e, "body"): logger.error("Exchange body: %s", e.body)
            if hasattr(e, "headers"): logger.error("Exchange headers: %s", e.headers)
            return False

        print(order)
        order_id = order.get('id', order)
        trade_param_list = self.bybit.fetch_order_trades(order_id, product_symbol)
        print(trade_param_list)

        record_list = self.record_to_df(trade_param_list)
        record_df = pd.DataFrame(record_list)
        record_df = record_df.drop(columns=record_df.columns[0])
        record_df['rec_time'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        record_df.set_index('rec_time', inplace=True)
        record_df = record_df.reindex(columns=self.col_order)
        write_header = not os.path.exists(self.file_path_trade) or os.path.getsize(self.file_path_trade) == 0
        record_df.to_csv(
            self.file_path_trade,
            mode='a',
            index=False,
            header=write_header
        )

        print('---------------------------------------------------------', symbol)

        return record_df
