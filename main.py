import schedule
import time
import datetime as dt
from loguru import logger

from core.orchestrator import DataSourceConfig
from core.datacenter import DataCenterSrv
from core.algo_strat import AlgoStrategy
from strategy.strat_method import CreateSignal
from core.execution import SignalExecution


def scheduler(bet_size):
    start_time = dt.datetime.now(dt.UTC)
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'Starting algo_seq at (UTC) {start_time_str}')

    # 1. Load strategy configuration
    ds = DataSourceConfig()
    ds.create_folder()
    strat_df = ds.load_info_dict()
    logger.info(f'Loaded #{len(strat_df)} rows of strategy configuration')

    # 2. Build request / data frame
    dcs = DataCenterSrv(strat_df)
    # dcs.create_df()
    dcs.create_bybit_df()
    logger.info('Data cleaning and update data complete')

    # 3. Collect market data
    algo = AlgoStrategy(strat_df)
    algo.data_collect()
    logger.info('Data collection completed')

    # 4. Generate trading signals
    gen_signal = CreateSignal(strat_df)
    signal_df = gen_signal.split_sub()
    logger.info(f'Generated {len(signal_df)} signals')

    # 5. Execute signals with per-symbol bet sizes
    signal_exec = SignalExecution(signal_df, BET_SIZE)
    signal_exec.create_market_order()
    logger.info(f'Executed market orders with bet_size mapping: {BET_SIZE}')

    end_time = dt.datetime.now(dt.UTC)
    duration = (end_time - start_time).total_seconds()
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'algo_seq finished at (UTC) {end_time_str} (duration: {round(duration, 1)} sec)')


if __name__ == '__main__':
    BET_SIZE = {'BTC': 0.01, 'ETH': 0.1, 'SOL': 0}
    xx_min = [1, 11, 21, 31, 41, 51]

    logger.info('Starting unified scheduler + algo program')

    for minute in xx_min:
        schedule.every().hour.at(f':{minute:02d}').do(scheduler, BET_SIZE)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning('KeyboardInterrupt received; program terminated.')