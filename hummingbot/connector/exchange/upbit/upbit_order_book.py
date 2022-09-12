import logging
import hummingbot.connector.exchange.upbit.upbit_constants as CONSTANTS

from typing import (
    Optional,
    Dict,
    List, Any)
from hummingbot.logger import HummingbotLogger
from hummingbot.core.event.events import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage, OrderBookMessageType
)
from hummingbot.connector.exchange.upbit.upbit_order_book_message import UpbitOrderBookMessage

_logger = None


class UpbitOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _logger
        if _logger is None:
            _logger = logging.getLogger(__name__)
        return _logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       trading_pair: str,
                                       timestamp: float,
                                       metadata: Optional[Dict] = None):
        """
        Convert json snapshot data into standard OrderBookMessage format
        :param msg: json snapshot data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :return: UpbitOrderBookMessage
        """

        if metadata:
            msg.update(metadata)
        return UpbitOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": timestamp,
                "bids": msg["bids"],
                "asks": msg["asks"]
            },
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None):
        """
        Convert json diff data into standard OrderBookMessage format
        :param msg: json diff data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :return: UpbitOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        return UpbitOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None):
        """
        Convert a trade data into standard OrderBookMessage format
        :param msg: json trade data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :return: UpbitOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        # Data fields are obtained from OrderTradeEvents
        msg.update({
            "trade_id": msg.get("sid"),
            "trade_type": float(TradeType.SELL.value) if msg["ab"] == "ask" else float(TradeType.BUY.value),
            "price": msg.get("tp"),
            "amount": msg.get("tv"),
            "update_id": msg.get("ttms")
        })

        return UpbitOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError(CONSTANTS.EXCHANGE_NAME + " order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(cls, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError(CONSTANTS.EXCHANGE_NAME + " order book needs to retain individual order data.")
