# collect_api.py

import requests
import pandas as pd
from time import sleep
import logging

# 로거 인스턴스를 가져옵니다 (main.py에서 설정된 로거를 사용).
logger = logging.getLogger(__name__)

HEADERS = {"accept": "application/json", "user-agent": "hourly-data/1.0"}

class ExchangeNotSupportedError(Exception):
    def __init__(self, exchange, supported):
        super().__init__(f"{exchange}는 지원하지 않습니다. 지원되는 거래소: {', '.join(supported)}")

def base_url_select(exchange):
    supported = {
        "upbit": "https://api.upbit.com",
        "bithumb": "https://api.bithumb.com"
    }
    key = exchange.lower()
    if key in supported:
        return supported[key]
    else:
        raise ExchangeNotSupportedError(exchange, list(supported.keys()))

def get_all_markets(exchange, only_krw=True):
    Base_url = base_url_select(exchange)
    url = f"{Base_url}/v1/market/all"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    markets = [d["market"] for d in data if isinstance(d, dict) and "market" in d]
    if only_krw:
        markets = [m for m in markets if m.startswith("KRW-")]
    return sorted(set(markets))

def fetch_hour_candles(exchange, market):
    Base_url = base_url_select(exchange)
    url = f"{Base_url}/v1/candles/minutes/60"
    params = {"market": market, "count": 1}
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()