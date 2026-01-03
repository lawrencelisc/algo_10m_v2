import pandas as pd
import numpy as np
import pytz
import sys
import os

from pathlib import Path
from loguru import logger
from datetime import datetime


class CreateSignal:
    # constant
    strat_list = ['zscore', 'ma_cross', 'bollinger', 'momentum', 'macd_quantile', 'zscore_atr']
    strat_folder = Path(__file__).parent.parent / 'data' / 'StratData'
    signal_folder = Path(__file__).parent.parent / 'data' / 'Signal'
    signal_filename = 'signal_table.csv'
    signal_path = signal_folder / signal_filename


    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df


    def _zscore_pos_loop(self, z: np.ndarray, mode: str, enter_thres: float,
                              exit_thres: float, confirm_bars: int, min_hold: int,
                              cooldown: int):
        n = len(z)
        pos = np.zeros(n, dtype=int)

        bar_pos = 0
        cooldown_num = 0
        up_count = 0
        dn_count = 0

        allow_long = mode in ('long', 'long_short')
        allow_short = mode in ('short', 'long_short')

        for i in range(n):
            zi = z[i]

            if np.isnan(zi):
                pos[i] = pos[i - 1] if i > 0 else 0
                continue

            prev_pos = pos[i - 1] if i > 0 else 0
            curr_pos = prev_pos

            # decrement cooldown
            if cooldown_num > 0:
                cooldown_num -= 1

            # update bars-in-position counter based on prev_pos
            if prev_pos != 0:
                bar_pos += 1
            else:
                bar_pos = 0

            can_exit = (bar_pos >= min_hold)

            # During position or cooldown, don't accumulate confirmation counts
            if prev_pos != 0 or cooldown_num > 0:
                up_count = 0
                dn_count = 0
            else:
                up_cond = (zi >= +abs(enter_thres))
                dn_cond = (zi <= -abs(enter_thres))
                up_count = up_count + 1 if up_cond else 0
                dn_count = dn_count + 1 if dn_cond else 0

            # exits
            if prev_pos == +1:
                if can_exit and zi <= +abs(exit_thres):
                    curr_pos = 0
                    if cooldown > 0:
                        cooldown_num = cooldown
                    bar_pos = 0
            elif prev_pos == -1:
                if can_exit and zi >= -abs(exit_thres):
                    curr_pos = 0
                    if cooldown > 0:
                        cooldown_num = cooldown
                    bar_pos = 0

            # entries
            if (curr_pos == 0) and (cooldown_num == 0):
                if allow_long and up_count >= confirm_bars:
                    curr_pos = +1
                    bar_pos = 0
                    up_count = dn_count = 0
                elif allow_short and dn_count >= confirm_bars:
                    curr_pos = -1
                    bar_pos = 0
                    up_count = dn_count = 0

            pos[i] = curr_pos
        return pos


    def strat_zscore(self, row):
        # ================== get param from su_table.csv
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])
        rol: int = int(row['rol'])

        enter_thres: float = float(row['thres'])
        exit_thres: float = float(row['exit_thres'])
        confirm_bars: int = int(row['confirm_bars'])
        min_hold: int = int(row['min_hold'])
        cooldown: int = int(row['cooldown'])

        strat_filename: str = f'{name}_{endpt_col}_{symbol}.csv'
        file_path = self.strat_folder / strat_filename
        df = pd.read_csv(file_path, index_col=0)
        df[endpt_col] = df[endpt_col].astype('float64')

        # ================== strategy calculation
        df['ma'] = df[endpt_col].rolling(rol).mean().astype('float64')
        df['std'] = df[endpt_col].rolling(rol).std().astype('float64')
        df[strat] = ((df[endpt_col] - df['ma']) / df['std']).astype('float64')

        z = df[strat].to_numpy(dtype=float)

        # call the loop function
        pos = self._zscore_pos_loop(z, mode, enter_thres, exit_thres,
                                         confirm_bars, min_hold, cooldown)

        df['pos'] = pos
        df.to_csv(file_path)
        signal = int(df['pos'].iloc[-1])

        return signal


    # gen signal table if strategy is ma cross
    def strat_ma_cross(self, row):
        signal: int = 0
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])
        short_rol: int = int(row['short_rol'])
        long_rol: int = int(row['long_rol'])
        strat_filename: str = f'{name}_{endpt_col}_{symbol}.csv'
        file_path = self.strat_folder / strat_filename

        df = pd.read_csv(file_path, index_col=0)
        df[endpt_col] = df[endpt_col].astype('float64')
        df[endpt_col] = df[endpt_col].replace('', np.nan)
        series = df[endpt_col].tolist()
        df['ewm_short'] = df[endpt_col].ewm(span=short_rol, min_periods=1).mean().astype('float64')
        df['sma_long'] = df[endpt_col].rolling(long_rol, min_periods=1).mean().astype('float64')

        if mode == 'long':
            df['pos'] = np.select([df['ewm_short'] > df['sma_long']], [1], default=0)
        elif mode == 'short':
            df['pos'] = np.select([df['ewm_short'] < df['sma_long']], [-1], default=0)
        elif mode == 'long_short':
            df['pos'] = np.select(
                [df['ewm_short'] > df['sma_long'], df['ewm_short'] < df['sma_long']],
                [1, -1],
                default=0
            )
        df.to_csv(file_path)
        signal = df['pos'].iloc[-1]

        return signal


    # gen signal table if strategy is bollinger
    def strat_bollinger(self, row):
        signal: int = 0
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])
        rol: int = int(row['rol'])
        num_std: float = float(row['num_std'])
        strat_filename: str = f'{name}_{endpt_col}_{symbol}.csv'
        file_path = self.strat_folder / strat_filename

        df = pd.read_csv(file_path, index_col=0)
        df[endpt_col] = df[endpt_col].astype('float64')
        df['ma'] = df[endpt_col].rolling(rol).mean().astype('float64')
        df['std'] = df[endpt_col].rolling(rol).std().astype('float64')
        df['std'] = np.where(df['std'] == 0, 1, df['std'])
        df['up'] = df['ma'] + (num_std * df['std'])
        df['dn'] = df['ma'] - (num_std * df['std'])

        if mode == 'long':
            df['pos'] = np.select(
                [df[endpt_col] > df['up'], df[endpt_col] < df['up']],
                [1, 0],
                default=0
            )
        elif mode == 'short':
            df['pos'] = np.select(
                [df[endpt_col] < df['dn'], df[endpt_col] > df['dn']],
                [-1, 0],
                default=0
            )
        elif mode == 'long_short':
            df['pos'] = np.select(
                [df[endpt_col] > df['up'], df[endpt_col] < df['dn']],
                [1, -1],
                default=0
            )
        df.to_csv(file_path)
        signal = df['pos'].iloc[-1]

        return signal


    # gen signal table if strategy is momentum
    def strat_momentum(self, row):
        signal: int = 0
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])
        rol: int = int(row['rol'])
        thres: float = float(row['thres'])
        strat_filename: str = f'{name}_{endpt_col}_{symbol}.csv'
        file_path = self.strat_folder / strat_filename

        df = pd.read_csv(file_path, index_col=0)
        df[endpt_col] = df[endpt_col].astype('float64')
        df['ma'] = df[endpt_col].rolling(rol).mean().astype('float64')
        df['ma_safe'] = np.where(df['ma'] == 0, 1, df['ma'])
        df['diff'] = ((df[endpt_col] - df['ma']) / df['ma_safe']) * 100

        if mode == 'long':
            df['pos'] = np.select([df['diff'] > thres], [1], default=0)
        elif mode == 'short':
            df['pos'] = np.select([df['diff'] < (-1 * thres)], [-1], default=0)
        elif mode == 'long_short':
            df['pos'] = np.select(
                [df['diff'] > thres, df['diff'] < (-1 * thres)],
                [1, -1],
                default=0
            )
        df.to_csv(file_path)
        signal = df['pos'].iloc[-1]

        return signal


    # gen signal table if strategy is macd quantile
    def strat_macd_quantile(self, row):

        # def ema(series: pd.Series, span: int):
        #    return series.ewm(span=span, adjust=False).mean()

        signal: int = 0
        fast: int = 12
        slow: int = 26
        signal_span: int = 9

        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])
        rol: int = int(row['rol'])
        thres: float = float(row['thres'])
        strat_filename: str = f'{name}_{endpt_col}_{symbol}.csv'
        file_path = self.strat_folder / strat_filename

        df = pd.read_csv(file_path, index_col=0)
        df[endpt_col] = df[endpt_col].astype('float64')
        df['ema_fast'] = df[endpt_col].ewm(span=fast, adjust=False).mean()
        df['ema_slow'] = df[endpt_col].ewm(span=slow, adjust=False).mean()
        df['macd'] = df['ema_fast'] - df['ema_slow']
        lower_quan = thres * 0.01
        upper_quan = 1 - thres * 0.01
        df2 = df['macd'].rolling(window=rol, min_periods=rol)
        df['macd_upper'] = df2.quantile(upper_quan, interpolation='linear')
        df['macd_lower'] = df2.quantile(lower_quan, interpolation='linear')

        if mode == 'long':
            df['pos'] = np.select([df['macd'] > df['macd_upper']], [1], default=0)
        elif mode == 'short':
            df['pos'] = np.select([df['macd'] < df['macd_lower']], [-1], default=0)
        elif mode == 'long_short':
            df['pos'] = np.select([df['macd'] > df['macd_upper'],
                                   df['macd'] < df['macd_lower']],
                                  [1, -1],
                                  default=0
                                  )
        df.to_csv(file_path)
        signal = df['pos'].iloc[-1]

        return signal


    def strat_zscore_atr(self, row):


        def zscore_close_pos(df, i, exit_price, reason, dir):
            idx = df.index[i]
            entry_price = df.loc[idx, 'entry_price']
            df.loc[idx, 'exit_price'] = exit_price

            # 計算損益
            if dir == 1:  # Long
                df.loc[idx, 'pnl'] = exit_price - entry_price
            else:  # Short
                df.loc[idx, 'pnl'] = entry_price - exit_price

            df.loc[idx, 'exit_reason'] = reason
            df.loc[idx, 'hold_days'] = df.loc[df.index[i - 1], 'hold_period'] + 1
            df.loc[idx, 'exit_pos'] = dir
            df.loc[idx, 'pos'] = 0
            df.loc[idx, 'hold_period'] = 0


        signal: int = 0
        atr_min: float = 0.3
        atr_max: float = 2.7
        name: str = str(row['name'])
        symbol: str = str(row['symbol'])
        endpt_col: str = str(row['endpt_col'])
        strat: str = str(row['strat'])
        mode: str = str(row['mode'])

        ma_rol: int = int(row['ma_rol'])
        atr_rol: int = int(row['atr_rol'])
        rol: int = int(row['rol'])
        thres_entry: float = float(row['thres'])
        thres_exit: float = float(row['exit_thres'])

        sl_multi: float = float(row['sl_multi'])
        tp_multi: float = float(row['tp_multi'])
        trailing_multi: float = float(row['trailing_multi'])
        max_hold: int = int(row['max_hold'])

        breakeven_threshold: float = float(row.get('breakeven_thres', 0.5))  # 默認 0.5 ATR
        use_sl: bool = sl_multi > 0
        use_trailing: bool = trailing_multi > 0
        use_breakeven: bool = breakeven_threshold > 0

        strat_filename: str = f'{name}_{endpt_col}_{symbol}.csv'
        file_path = self.strat_folder / strat_filename

        raw_df = pd.read_csv(file_path, index_col=0, parse_dates=True)
        raw_df.rename(columns={'c': 'close', 'h': 'high', 'l': 'low', 'o': 'open'}, inplace=True)

        # 計算變化率
        raw_df['chg'] = raw_df['close'].pct_change()

        # 趨勢線 (移動平均) trend ma
        raw_df['trend_ma'] = raw_df['close'].rolling(window=ma_rol).mean()

        # ATR (平均真實波幅)
        raw_df['high_low'] = raw_df['high'] - raw_df['low']
        raw_df['high_close'] = abs(raw_df['high'] - raw_df['close'].shift(1))
        raw_df['low_close'] = abs(raw_df['low'] - raw_df['close'].shift(1))
        raw_df['tr'] = raw_df[['high_low', 'high_close', 'low_close']].max(axis=1)
        raw_df['atr'] = raw_df['tr'].rolling(window=atr_rol).mean()
        raw_df['atr_pct'] = (raw_df['atr'] / raw_df['close']) * 100

        df = raw_df.copy()

        # 計算 Z-Score
        df['zscore'] = (
                (df['close'] - df['close'].rolling(rol).mean()) /
                df['close'].rolling(rol).std()
        )

        # 初始化追蹤欄位
        df['pos'] = 0                                       # 持倉方向 (1=Long, -1=Short, 0=空倉)
        df['hold_period'] = 0                               # 當前持倉期數
        df['hold_days'] = 0                                 # 完成交易的持倉期數
        df['entry_price'] = np.nan                          # 進場價格
        df['entry_atr'] = np.nan                            # 進場時的 ATR
        df['best_price'] = np.nan                           # Track best price achieved
        df['exit_price'] = np.nan                           # 出場價格
        df['pnl'] = 0.0                                     # 單筆損益
        df['exit_reason'] = ''                              # 出場原因
        df['exit_pos'] = 0                                  # 出場時的方向

        for i in range(1, len(df)):
            idx = df.index[i]
            prev_idx = df.index[i - 1]
            prev_pos = df.loc[prev_idx, 'pos']

            # ========== Long 平倉邏輯 ==========
            if prev_pos == 1:
                df.loc[idx, 'pos'] = 1
                df.loc[idx, 'hold_period'] = df.loc[prev_idx, 'hold_period'] + 1
                df.loc[idx, 'entry_price'] = df.loc[prev_idx, 'entry_price']
                df.loc[idx, 'entry_atr'] = df.loc[prev_idx, 'entry_atr']

                entry_price = df.loc[idx, 'entry_price']
                entry_atr = df.loc[idx, 'entry_atr']

                # Update best price for long (highest high achieved)
                prev_best = df.loc[prev_idx, 'best_price']
                if pd.isna(prev_best):
                    df.loc[idx, 'best_price'] = df.loc[idx, 'high']
                else:
                    df.loc[idx, 'best_price'] = max(prev_best, df.loc[idx, 'high'])

                best_price = df.loc[idx, 'best_price']
                # Calculate profit in ATR multiples
                profit_in_atr = (best_price - entry_price) / entry_atr if entry_atr > 0 else 0

                # 🆕 計算當前未實現獲利 (用於盈虧平衡止損)
                current_profit_atr = (df.loc[idx, 'close'] - entry_price) / entry_atr if entry_atr > 0 else 0

                # ===== 出場優先級 =====

                # 1. 止盈 (最優先)
                if df.loc[idx, 'high'] >= entry_price + (tp_multi * entry_atr):
                    zscore_close_pos(df, i, entry_price + (tp_multi * entry_atr), 'TP', 1)

                # 2. 追蹤止損 (Trailing Stop) - CHECK FIRST before other exits
                elif use_trailing and profit_in_atr >= 1.0 and \
                        df.loc[idx, 'low'] <= best_price - (trailing_multi * entry_atr):
                    zscore_close_pos(df, i, best_price - (trailing_multi * entry_atr), 'TRAIL', 1)

                # 3. 盈虧平衡止損 (獲利達標但未達追蹤止損條件)
                elif use_breakeven and current_profit_atr >= breakeven_threshold and \
                        df.loc[idx, 'low'] <= entry_price:
                    zscore_close_pos(df, i, entry_price, 'BREAKEVEN', 1)

                # 4. Z-Score 回歸
                elif df.loc[idx, 'zscore'] >= -thres_exit:
                    zscore_close_pos(df, i, df.loc[idx, 'close'], 'ZSCORE', 1)

                # 5. 時間止損
                elif df.loc[idx, 'hold_period'] >= max_hold:
                    zscore_close_pos(df, i, df.loc[idx, 'close'], 'TIME', 1)

                # 6. 止損
                elif use_sl and df.loc[idx, 'low'] <= entry_price - (sl_multi * entry_atr):
                    zscore_close_pos(df, i, entry_price - (sl_multi * entry_atr), 'SL', 1)

            # ========== Short 平倉邏輯 ==========
            elif prev_pos == -1:
                df.loc[idx, 'pos'] = -1
                df.loc[idx, 'hold_period'] = df.loc[prev_idx, 'hold_period'] + 1
                df.loc[idx, 'entry_price'] = df.loc[prev_idx, 'entry_price']
                df.loc[idx, 'entry_atr'] = df.loc[prev_idx, 'entry_atr']

                entry_price = df.loc[idx, 'entry_price']
                entry_atr = df.loc[idx, 'entry_atr']

                # Update best price for short (lowest low achieved)
                prev_best = df.loc[prev_idx, 'best_price']
                if pd.isna(prev_best):
                    df.loc[idx, 'best_price'] = df.loc[idx, 'low']
                else:
                    df.loc[idx, 'best_price'] = min(prev_best, df.loc[idx, 'low'])

                best_price = df.loc[idx, 'best_price']
                profit_in_atr = (entry_price - best_price) / entry_atr if entry_atr > 0 else 0

                # 🆕 計算當前未實現獲利
                current_profit_atr = (entry_price - df.loc[idx, 'close']) / entry_atr if entry_atr > 0 else 0

                # ===== 出場優先級 =====

                # 1. 止盈
                if df.loc[idx, 'low'] <= entry_price - (tp_multi * entry_atr):
                    zscore_close_pos(df, i, entry_price - (tp_multi * entry_atr), 'TP', -1)

                # 2. 追蹤止損 (Trailing Stop) - CHECK FIRST before other exits
                elif use_trailing and profit_in_atr >= 1.0 and \
                        df.loc[idx, 'high'] >= best_price + (trailing_multi * entry_atr):
                    zscore_close_pos(df, i, best_price + (trailing_multi * entry_atr), 'TRAIL', -1)

                # 3. 盈虧平衡止損
                elif use_breakeven and current_profit_atr >= breakeven_threshold and \
                        df.loc[idx, 'high'] >= entry_price:
                    zscore_close_pos(df, i, entry_price, 'BREAKEVEN', -1)

                # 4. Z-Score 回歸
                elif df.loc[idx, 'zscore'] <= thres_exit:
                    zscore_close_pos(df, i, df.loc[idx, 'close'], 'ZSCORE', -1)

                # 5. 時間止損
                elif df.loc[idx, 'hold_period'] >= max_hold:
                    zscore_close_pos(df, i, df.loc[idx, 'close'], 'TIME', -1)

                # 6. 止損
                elif use_sl and df.loc[idx, 'high'] >= entry_price + (sl_multi * entry_atr):
                    zscore_close_pos(df, i, entry_price + (sl_multi * entry_atr), 'SL', -1)

            # ========== 開倉邏輯 ==========
            elif prev_pos == 0:
                allow_long = mode in ['long_short', 'long']
                allow_short = mode in ['long_short', 'short']

                # Long 開倉
                if allow_long and df.loc[idx, 'zscore'] < (-1 * thres_entry):
                    if (atr_min <= df.loc[idx, 'atr_pct'] <= atr_max) and \
                            (df.loc[idx, 'close'] < df.loc[idx, 'trend_ma']):
                        df.loc[idx, 'pos'] = 1
                        df.loc[idx, 'hold_period'] = 0
                        df.loc[idx, 'entry_price'] = df.loc[idx, 'close']
                        df.loc[idx, 'entry_atr'] = df.loc[idx, 'atr']
                        df.loc[idx, 'best_price'] = df.loc[idx, 'high']  # ADD THIS LINE

                # Short 開倉
                elif allow_short and df.loc[idx, 'zscore'] > thres_entry:
                    if (atr_min <= df.loc[idx, 'atr_pct'] <= atr_max) and \
                            (df.loc[idx, 'close'] > df.loc[idx, 'trend_ma']):
                        df.loc[idx, 'pos'] = -1
                        df.loc[idx, 'hold_period'] = 0
                        df.loc[idx, 'entry_price'] = df.loc[idx, 'close']
                        df.loc[idx, 'entry_atr'] = df.loc[idx, 'atr']
                        df.loc[idx, 'best_price'] = df.loc[idx, 'low']

        df.to_csv(file_path)
        signal = df['pos'].iloc[-1]

        return signal


    def split_sub(self):
        count = 0
        new_signal_df = pd.DataFrame()
        combine_signal_df = pd.DataFrame(
            {
                'name': pd.Series(dtype='string'),
                'symbol': pd.Series(dtype='string'),
                'saved_csv': pd.Series(dtype='string'),
                'signal': pd.Series(dtype='int64'),
            }
        )

        strategy_funcs = {
            self.strat_list[0]: self.strat_zscore,                  # 0 - zscore
            self.strat_list[1]: self.strat_ma_cross,                # 1 - ma_cross
            self.strat_list[2]: self.strat_bollinger,               # 2 - bollinger
            self.strat_list[3]: self.strat_momentum,                # 3 - momentum
            self.strat_list[4]: self.strat_macd_quantile,           # 4 - macd quantile
            self.strat_list[5]: self.strat_zscore_atr,              # 5 - zscore_atr
        }

        for _, row in self.strat_df.iterrows():
            func = strategy_funcs.get(row['strat'])
            if func:
                func(row)
                count += 1
                str_saved_csv = str(row['name'] + '_' + row['endpt_col'] + '_' + row['symbol'] + '.csv')
                strat_path = self.strat_folder / str_saved_csv
                df = pd.read_csv(strat_path, index_col=0)
                lastest_date = str(df.index[-1])

                new_row = {
                    'date': lastest_date,
                    'name': str(row['name']),
                    'symbol': str(row['symbol']),
                    'saved_csv': str_saved_csv,
                    'signal': str(func(row))
                }

                new_row_df = pd.DataFrame([new_row])
                new_row_df['date'] = pd.to_datetime(new_row_df['date'])
                new_row_df = new_row_df.set_index('date')

                frames = [df for df in [combine_signal_df, new_row_df] if df is not None and not df.empty]
                if frames:
                    combine_signal_df = pd.concat(frames, axis=0)
                else:
                    combine_signal_df = pd.DataFrame()

                # Try to load existing CSV if it exists
                if os.path.exists(self.signal_path):
                    try:
                        existing_signal_df = pd.read_csv(self.signal_path)
                    except pd.errors.EmptyDataError:
                        existing_signal_df = pd.DataFrame()
                        logger.error(f'Failed to read existing CSV {self.signal_filename}: {e}')
                else:
                    existing_signal_df = pd.DataFrame()

        # If the CSV is empty or missing, create it
        combine_signal_df = combine_signal_df.reset_index()
        if existing_signal_df.empty:
            combine_signal_df.to_csv(self.signal_path, index=False)
        else:
            combined_df = pd.concat([existing_signal_df, combine_signal_df], ignore_index=True)
            combined_df.to_csv(self.signal_path, index=False)
        logger.info(f'Updated {self.signal_filename}: +{count} new rows.')

        return combine_signal_df