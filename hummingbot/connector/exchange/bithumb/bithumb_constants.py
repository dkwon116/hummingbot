# A single source of truth for constant variables related to the exchange
from hummingbot.core.api_throttler.data_types import RateLimit, LinkedLimitWeightPair

EXCHANGE_NAME = "bithumb"

REST_URLS = {"bithumb_main": "https://api.bithumb.com"}
WSS_URLS = {"bithumb_main": "wss://pubwss.bithumb.com/pub/ws"}

REST_API_VERSION = "v1.3"

# REST API Public Endpoints
GET_MARKETS_URL = "/public/ticker/ALL_KRW"
GET_TICKER_URL = "/public/ticker/ALL_KRW"
GET_ORDER_BOOK_URL = "/public/orderbook/"

# REST API ENDPOINTS
GET_ACCOUNT_SUMMARY_PATH_URL = "/info/balance"
CREATE_LIMIT_ORDER_PATH_URL = "/trade/place"
CREATE_MARKET_BUY_ORDER_PATH_URL = "/trade/market_buy"
CREATE_MARKET_SELL_ORDER_PATH_URL = "/trade/market_sell"
CANCEL_ORDER_PATH_URL = "/trade/cancel"
GET_ORDER_DETAIL_PATH_URL = "/info/order_detail"
GET_OPEN_ORDERS_PATH_URL = "/info/orders"


# WebSocket Public Endpoints
WS_PING_REQUEST = "Ping"
WS_PING_ID = "WSPing"

API_LIMIT_REACHED_ERROR_MESSAGE = "TOO MANY REQUESTS"

MINUTE = 60
HTTP_ENDPOINTS_LIMIT_ID = "AllHTTP"
HTTP_LIMIT = 900
PUBLIC_HTTP_LIMIT = 8100
PRIVATE_HTTP_LIMIT = 900
WS_AUTH_LIMIT_ID = "AllWsAuth"
WS_ENDPOINTS_LIMIT_ID = "AllWs"
WS_LIMIT = 500
RATE_LIMITS = [
    RateLimit(limit_id=HTTP_ENDPOINTS_LIMIT_ID, limit=HTTP_LIMIT, time_interval=MINUTE),
    # public http
    RateLimit(
        limit_id=GET_MARKETS_URL,
        limit=PUBLIC_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_ORDER_BOOK_URL,
        limit=PUBLIC_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_TICKER_URL,
        limit=PUBLIC_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    # private http
    RateLimit(
        limit_id=GET_ACCOUNT_SUMMARY_PATH_URL,
        limit=PRIVATE_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CREATE_LIMIT_ORDER_PATH_URL,
        limit=PRIVATE_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CREATE_MARKET_BUY_ORDER_PATH_URL,
        limit=PRIVATE_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CREATE_MARKET_SELL_ORDER_PATH_URL,
        limit=PRIVATE_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=CANCEL_ORDER_PATH_URL,
        limit=PRIVATE_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_ORDER_DETAIL_PATH_URL,
        limit=PRIVATE_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    RateLimit(
        limit_id=GET_OPEN_ORDERS_PATH_URL,
        limit=PRIVATE_HTTP_LIMIT,
        time_interval=MINUTE,
        linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)],
    ),
    # ws public
    RateLimit(limit_id=WS_AUTH_LIMIT_ID, limit=50, time_interval=MINUTE),
    RateLimit(limit_id=WS_ENDPOINTS_LIMIT_ID, limit=WS_LIMIT, time_interval=MINUTE)
]
