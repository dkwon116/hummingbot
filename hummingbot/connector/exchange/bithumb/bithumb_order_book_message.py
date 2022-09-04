#!/usr/bin/env python

from typing import (
    Dict,
    List,
    Optional,
)

from hummingbot.core.data_type.order_book_row import OrderBookRow
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage,
    OrderBookMessageType,
)


class BithumbOrderBookMessage(OrderBookMessage):

    _BUY_SIDE = "bid"
    _SELL_SIDE = "ask"

    def __new__(
        cls,
        message_type: OrderBookMessageType,
        content: Dict[str, any],
        timestamp: Optional[float] = None,
        *args,
        **kwargs,
    ):
        if timestamp is None:
            if message_type is OrderBookMessageType.SNAPSHOT:
                raise ValueError("timestamp must not be None when initializing snapshot messages.")
            timestamp = content["timestamp"]

        return super(BithumbOrderBookMessage, cls).__new__(
            cls, message_type, content, timestamp=timestamp, *args, **kwargs
        )

    @property
    def update_id(self) -> int:
        return int(self.timestamp * 1e3)
        # if self.type == OrderBookMessageType.SNAPSHOT:
        #     # Assumes Snapshot Update ID to be 0
        #     # Since uid of orderbook snapshots from REST API is not in sync with uid from websocket
        #     return -1
        # elif self.type in [OrderBookMessageType.DIFF, OrderBookMessageType.TRADE]:
        #     return int(self.timestamp * 1e3)

    @property
    def trade_id(self) -> int:
        if self.type is OrderBookMessageType.TRADE:
            return int(self.timestamp * 1e3)
        return -1

    @property
    def trading_pair(self) -> str:
        return self.content["trading_pair"]

    @property
    def last_traded_price(self) -> float:
        entries = self.content["price"]
        return float(entries[-1].lastTradePrice)

    @property
    def asks(self) -> List[OrderBookRow]:
        asks = [self._order_book_row_for_entry(entry) for entry in self.content["asks"]]
        asks.sort(key=lambda row: (row.price, row.update_id))
        return asks

    @property
    def bids(self) -> List[OrderBookRow]:
        bids = [self._order_book_row_for_entry(entry) for entry in self.content["bids"]]
        bids.sort(key=lambda row: (row.price, row.update_id))
        return bids

    def _order_book_row_for_entry(self, entry) -> OrderBookRow:
        price = float(entry[0])
        amount = float(entry[1])
        update_id = float(entry[2])
        return OrderBookRow(price, amount, update_id)

    def __eq__(self, other) -> bool:
        return type(self) == type(other) and self.type == other.type and self.timestamp == other.timestamp

    def __lt__(self, other) -> bool:
        # If timestamp is the same, the ordering is snapshot < diff < trade
        return (self.timestamp < other.timestamp or (self.timestamp == other.timestamp and self.type.value < other.type.value))

    def __hash__(self) -> int:
        return hash((self.type, self.timestamp))
