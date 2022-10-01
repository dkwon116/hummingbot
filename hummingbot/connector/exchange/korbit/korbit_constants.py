import sys

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

HBOT_BROKER_ID = "DK-"
MAX_ORDER_ID_LEN = None

DEFAULT_DOMAIN = ""
KORBIT_BASE_URL = "https://api.korbit.co.kr/v1"
KORBIT_WS_URL = "wss://ws.korbit.co.kr/v1/user/push"

# Public endpoints
KORBIT_NETWORK_STATUS_PATH = "/ticker"
KORBIT_ORDER_BOOK_PATH = "/orderbook"
KORBIT_MARKETS_PATH = "/ticker/detailed/all"
KORBIT_SINGLE_MARKET_PATH = "/ticker"

# Private endpoints (require authentication)
KORBIT_AUTH_PATH = "/oauth2/access_token"
KORBIT_ORDERS_PATH = "/user/orders"
KORBIT_BALANCES_PATH = "/user/balances"

WS_PING_INTERVAL = 15
WS_TRADES_CHANNEL = "transaction"
WS_ORDER_BOOK_CHANNEL = "orderbook"

WS_EVENT_UPDATE_TYPE = "update"
WS_EVENT_ERROR_TYPE = "error"
WS_EVENT_ERROR_CODE = 400
WS_EVENT_NOT_LOGGED_IN_MESSAGE = "Not logged in"
WS_EVENT_INVALID_LOGIN_MESSAGE = "Invalid login credentials"

NO_LIMIT = sys.maxsize
# KORBIT_NETWORK_STATUS_LIMIT_ID = "KORBITNetworkStatusHTTPRequest"
KORBIT_ORDER_BOOK_LIMIT_ID = "KORBITOrderBookHTTPRequest"
KORBIT_GET_ORDER_LIMIT_ID = "KORBITGetOrderHTTPRequest"
KORBIT_CANCEL_ORDER_LIMIT_ID = "KORBITCancelOrderHTTPRequest"
WS_CONNECTION_LIMIT_ID = "KORBITWSConnection"
WS_REQUEST_LIMIT_ID = "KORBITWSRequest"
KORBIT_PER_SECOND_ORDER_SPOT_LIMIT_ID = "KORBITPerSecondOrderSpot"
KORBIT_PER_MS_ORDER_SPOT_LIMIT_ID = "KORBITPerMSOrderSpot"


HTTP_ENDPOINTS_LIMIT_ID = "AllHTTP"
TICKER_LIMIT_ID = "AllTicker"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 5000

RATE_LIMITS = [
    RateLimit(limit_id=HTTP_ENDPOINTS_LIMIT_ID, limit=30, time_interval=ONE_SECOND),
    RateLimit(limit_id=TICKER_LIMIT_ID, limit=60, time_interval=ONE_MINUTE),

    # Weighted Limits
    RateLimit(limit_id=KORBIT_MARKETS_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(TICKER_LIMIT_ID)]),
    RateLimit(limit_id=KORBIT_MARKETS_PATH, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[(LinkedLimitWeightPair(TICKER_LIMIT_ID))]),
    RateLimit(limit_id=KORBIT_ORDER_BOOK_PATH, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=KORBIT_NETWORK_STATUS_PATH, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=KORBIT_BALANCES_PATH, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)]),
    RateLimit(limit_id=KORBIT_ORDERS_PATH, limit=MAX_REQUEST, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HTTP_ENDPOINTS_LIMIT_ID)]),
]