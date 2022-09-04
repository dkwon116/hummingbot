import asyncio
import unittest
import aiohttp

# from hummingbot.connector.exchange.upbit.upbit_auth import UpbitAuth
# from hummingbot.connector.exchange.upbit.upbit_websocket_adaptor import UpbitWebSocketAdaptor


class TestAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.api_key = "i1MWfJCHO4cDJcuDWRSp5mzpTkIA5dgXsar6Uh25"
        cls.secret_key = "boW5w39uq59SxU1pIZFEC6XLfJ7bQyTx0JoTDjXk"
        # cls.client = UpbitExchange._http_client(self)
        # cls.ws = UpbitWebSocketAdaptor(cls.auth)

    def test_auth(self):
        # headers = UpbitAuth(self.api_key, self.secret_key).get_auth_headers()
        response = aiohttp.ClientSession().get("https://api.upbit.com/v1/accounts")
        assert response["currency"] == "KRW"
