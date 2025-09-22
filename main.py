# main.py

import logging
import os
import pandas as pd
from time import sleep

# 다른 파일에서 필요한 함수들을 가져옵니다.
from collect_api import get_all_markets, fetch_hour_candles
from db_handler import save_to_supabase
# main.py

import logging
import os
import pandas as pd
from time import sleep

# 다른 파일에서 필요한 함수들을 가져옵니다.
from collect_api import get_all_markets, fetch_hour_candles
from db_handler import save_to_supabase
ㄱ
# --- 로거(Logger) 설정 ---
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/data_collection.log')
    ]
)
logger = logging.getLogger(__name__)

def run_collection_process(exchange, only_krw=True):
    """특정 거래소의 데이터를 수집하고 DB에 저장하는 전체 프로세스"""
    logger.info(f"'{exchange}' 거래소 데이터 수집을 시작합니다.")
    try:
        markets = get_all_markets(exchange=exchange, only_krw=only_krw)
        logger.info(f"총 {len(markets)}개의 마켓을 대상으로 데이터를 수집합니다.")
    except Exception as e:
        logger.error(f"마켓 목록을 가져오는 데 실패했습니다: {e}", exc_info=True)
        return

    rows_out = []
    for i, mkt in enumerate(markets, 1):
        try:
            data = fetch_hour_candles(exchange=exchange, market=mkt)
            if not data or not isinstance(data, list):
                logger.warning(f"'{mkt}' 데이터가 없거나 형식이 올바르지 않음: {data}")
                continue
            
            rows_out.append({
                "market": mkt,
                "datetime_kst": data[0]["candle_date_time_kst"],
                "traded_price": data[0]["candle_acc_trade_price"]
            })
        except Exception as e:
            logger.error(f"'{mkt}' 페어 캔들 수집 실패: {e}")
        
        sleep(0.1)
        if i % 100 == 0:
            logger.info(f"진행 상황: {i}/{len(markets)}")

    if not rows_out:
        logger.warning("수집된 데이터가 없습니다.")
        return

    df = pd.DataFrame(rows_out)
    
    # DB에 저장
    save_to_supabase(df, exchange_name=exchange)
    logger.info(f"'{exchange}' 거래소 데이터 수집 및 저장을 완료했습니다.")

if __name__ == "__main__":
    # .env 파일에 SUPABASE_URL과 SUPABASE_SERVICE_ROLE_KEY를 설정해야 합니다.
    run_collection_process(exchange="upbit", only_krw=True)
    run_collection_process(exchange="bithumb", only_krw=True)
    logger.info("모든 작업이 완료되었습니다.")