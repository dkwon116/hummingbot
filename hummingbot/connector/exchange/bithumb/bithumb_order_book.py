import logging
import hummingbot.connector.exchange.bithumb.bithumb_constants as CONSTANTS

from sqlalchemy.engine import RowProxy
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
from hummingbot.connector.exchange.bithumb.bithumb_order_book_message import BithumbOrderBookMessage

_logger = None


class BithumbOrderBook(OrderBook):
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
        :return: BithumbOrderBookMessage
        """

        if metadata:
            msg.update(metadata)
        return BithumbOrderBookMessage(
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
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of snapshot data into standard OrderBookMessage format
        :param record: a row of snapshot data from the database
        :return: BithumbOrderBookMessage
        """
        return BithumbOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=record.json,
            timestamp=record.timestamp
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
        :return: BithumbOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        return BithumbOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content={
                "trading_pair": msg["trading_pair"],
                "update_id": timestamp,
                "bids": msg["bids"],
                "asks": msg["asks"]
            },
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of diff data into standard OrderBookMessage format
        :param record: a row of diff data from the database
        :return: BithumbOrderBookMessage
        """
        return BithumbOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=record.json,
            timestamp=record.timestamp
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
        :return: BithumbOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        # Data fields are obtained from OrderTradeEvents
        msg.update({
            "trade_id": int(msg.get("contDtm") * 1000000),
            "trade_type": float(TradeType.SELL.value) if msg["buySellGb"] == "1" else float(TradeType.BUY.value),
            "price": float(msg.get("contPrice")),
            "amount": float(msg.get("contQty")),
            "update_id": int(msg.get("contDtm") * 1000)
        })

        return BithumbOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of trade data into standard OrderBookMessage format
        :param record: a row of trade data from the database
        :return: BithumbOrderBookMessage
        """
        return BithumbOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=record.json,
            timestamp=record.timestamp
        )

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError(CONSTANTS.EXCHANGE_NAME + " order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(cls, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError(CONSTANTS.EXCHANGE_NAME + " order book needs to retain individual order data.")
