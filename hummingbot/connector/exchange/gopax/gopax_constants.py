# A single source of truth for constant variables related to the exchange
from hummingbot.core.api_throttler.data_types import RateLimit, LinkedLimitWeightPair

EXCHANGE_NAME = "gopax"

REST_URLS = {"gopax_main": "https://api.gopax.co.kr"}
WSS_URLS = {"gopax_main": "wss://wsapi.gopax.co.kr"}

REST_API_VERSION = "v1.3"

# REST API Public Endpoints
GET_MARKETS_URL = "/trading-pairs"
GET_TICKER_URL = "/trading-pairs/stats"
GET_ORDER_BOOK_URL = "/trading-pairs/"
GET_TIME_URL = "/time"

# REST API ENDPOINTS
GET_TRADING_RULES_PATH_URL = "orders/chance"
CREATE_ORDER_PATH_URL = "/orders"
CANCEL_ORDER_PATH_URL = "/orders"
GET_ORDER_DETAIL_PATH_URL = "/orders"
GET_OPEN_ORDERS_PATH_URL = "/orders"
GET_ACCOUNT_SUMMARY_PATH_URL = "/balances"
GET_TRADES_HISTORY_PATH_URL = "/trades"

# WebSocket Public Endpoints
WS_PING = 'primus::ping::'
WS_PONG = 'primus::pong::'

API_LIMIT_REACHED_ERROR_MESSAGE = "TOO MANY REQUESTS"

SECOND = 1
MINUTE = 60
HTTP_ENDPOINTS_LIMIT_ID = "AllHTTP"
HTTP_LIMIT = 100
WS_AUTH_LIMIT_ID = "AllWsAuth"
WS_ENDPOINTS_LIMIT_ID = "AllWs"
WS_LIMIT = 500
RATE_LIMITS = [
    RateLimit(limit_id=HTTP_ENDPOINTS_LIMIT_ID, limit=HTTP_LIMIT, time_interval=SECOND),
    # public http
    RateLimit(
        limit_id=GET_MARKETS_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_ORDER_BOOK_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_TICKER_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_TIME_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    # private http
    RateLimit(
        limit_id=GET_ACCOUNT_SUMMARY_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_TRADING_RULES_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CREATE_ORDER_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CANCEL_ORDER_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_ORDER_DETAIL_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_OPEN_ORDERS_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_TRADES_HISTORY_PATH_URL,
        limit=HTTP_LIMIT,
        time_interval=SECOND,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    # ws public
    RateLimit(limit_id=WS_AUTH_LIMIT_ID, limit=50, time_interval=MINUTE),
    RateLimit(limit_id=WS_ENDPOINTS_LIMIT_ID, limit=WS_LIMIT, time_interval=MINUTE)
]
