import requests
from abc import ABC, abstractmethod
import logging


pmm_logger = None

class TaapiIndicators(ABC):
    @classmethod
    def logger(cls):
        global pmm_logger
        if pmm_logger is None:
            pmm_logger = logging.getLogger(__name__)
        return pmm_logger

    def __init__(self):
        self._base_api = "https://api.taapi.io/bulk"
        self._secret = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImRrd29uODYxMTZAZ21haWwuY29tIiwiaWF0IjoxNjQwNjc4MjY5LCJleHAiOjc5NDc4NzgyNjl9.4omExfgJZkas2CtRUFpJAt3UaqJZY0_2w3ywwKoA1X8"
        
    def build_base_params(self, symbol, interval, indicators):
        param = {
            'secret': self._secret,
            'construct': {
                'exchange': 'binance',
                'symbol': symbol,
                'interval': interval,
                'indicators': indicators
            }
        }

        return param

    def get_indicators(self, symbol, interval, indicators):
        rsp = requests.post(url = self._base_api, json = self.build_base_params(symbol, interval, indicators))
        
        result = rsp.json()
        return result["data"]