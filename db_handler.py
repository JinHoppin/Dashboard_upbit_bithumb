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
    """최근 N시간 동안의 거래량 데이터를 DB에서 가져옵니다."""
    try:
        supabase = get_supabase()

        # 현재 시간에서 hours 만큼 이전 시간 계산
        from datetime import datetime, timedelta, timezone
        time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Supabase에서 데이터 조회
        res = supabase.table("hourly_volume") \
                    .select("exchange, market, datetime_kst, traded_price") \
                    .gte("datetime_kst", time_threshold.isoformat()) \
                    .execute()

        df = pd.DataFrame(res.data)
        logger.info(f"[Supabase] {len(df)}개의 데이터를 성공적으로 조회했습니다.")
        return df

    except Exception as e:
        logger.error(f"[Supabase] 데이터 조회 실패: {e}", exc_info=True)
        return pd.DataFrame() # 실패 시 빈 데이터프레임 반환