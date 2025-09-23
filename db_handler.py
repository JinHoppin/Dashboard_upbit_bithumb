# db_handler.py

import os
import time
import logging
from supabase import create_client
from dotenv import load_dotenv
import pandas as pd

logger = logging.getLogger(__name__)

def get_supabase():
    """Supabase 클라이언트를 생성하고 반환합니다."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise ValueError("Supabase URL 또는 Key가 .env 파일에 설정되지 않았습니다.")
    logger.info("[Supabase] 클라이언트 초기화 완료.")
    return create_client(url, key)

def save_to_supabase(df: pd.DataFrame, exchange_name: str):
    """DataFrame을 Supabase에 'upsert'합니다."""
    if df.empty:
        logger.warning("저장할 데이터가 없어 Supabase 업로드를 건너뜁니다.")
        return

    df['exchange'] = exchange_name
    records = df.to_dict(orient='records')

    supabase = get_supabase()
    conflict_columns = 'exchange, market, datetime_kst'
    table_name = "hourly_volume"

    logger.info(f"[Supabase] Upsert 시작: Table='{table_name}', Rows={len(records)}")
    t0 = time.perf_counter()
    try:
        res = supabase.table(table_name).upsert(records, on_conflict=conflict_columns).execute()
        t1 = time.perf_counter()
        upserted_count = len(res.data) if hasattr(res, 'data') and res.data else 0
        logger.info(f"[Supabase] Upsert 완료: {upserted_count}개 행 처리, 소요시간={t1 - t0:.2f}초")
    except Exception as e:
        logger.error(f"[Supabase] Upsert 실패: {e}", exc_info=True)

def fetch_hourly_volume(hours=24):
    """
    최근 N시간 동안의 모든 거래량 데이터를 DB에서 가져옵니다. (1000개 제한 및 페이지네이션 처리)
    """
    try:
        supabase = get_supabase()
        from datetime import datetime, timedelta, timezone
        
        KST = timezone(timedelta(hours=9))
        now_kst = datetime.now(KST)
        time_threshold = now_kst - timedelta(hours=hours)

        all_data = []
        current_page = 0
        page_size = 1000  # Supabase의 기본 최대 한도

        while True:
            # 1. 페이지별로 데이터 조회 범위 설정
            start_index = current_page * page_size
            end_index = start_index + page_size - 1

            res = supabase.table("hourly_volume") \
                        .select("exchange, market, datetime_kst, traded_price", count='exact') \
                        .gte("datetime_kst", time_threshold.isoformat()) \
                        .range(start_index, end_index) \
                        .execute()
            
            # 2. 조회된 데이터를 리스트에 추가
            fetched_data = res.data
            if fetched_data:
                all_data.extend(fetched_data)
            
            # 3. 더 이상 가져올 데이터가 없으면 반복문 종료
            if len(fetched_data) < page_size:
                break
            
            current_page += 1

        df = pd.DataFrame(all_data)
        logger.info(f"[Supabase] 총 {len(df)}개({current_page + 1} 페이지)의 데이터를 성공적으로 조회했습니다.")
        return df

    except Exception as e:
        logger.error(f"[Supabase] 데이터 조회 실패: {e}", exc_info=True)
        return pd.DataFrame()
