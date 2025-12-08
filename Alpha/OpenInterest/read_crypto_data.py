import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("BAGGERLAB_WJ_API_KEY")
API_SECRET = os.getenv("BAGGERLAB_WJ_API_SECRET_KEY")

SPOT_URL = "https://api.binance.com"   # Spot (현물)
FUTURES_URL = "https://fapi.binance.com"  # USDT-M Futures

# 15분 캔들 기준 500개 (바이낸스 API는 최대 500개 제공)
params = {
    "symbol": "BTCUSDT",
    "period": "5m",     # 5m, 15m, 1h, 4h, 1d etc..
    "limit": 500        # max 500
}

# 현재 OI : /fapi/v1/openInterest
# 과거 OI : /futures/data/openInterestHist
r = requests.get(f"{FUTURES_URL}/futures/data/openInterestHist", params=params)
print(r.json())