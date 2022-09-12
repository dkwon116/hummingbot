from decimal import Decimal
from typing import Optional, Dict, Any, List

from hummingbot.core.data_type.common import OrderType, TradeType, PositionAction
from hummingbot.connector.derivative.ftx_perp.ftx_perp_order_status import FtxPerpOrderStatus
from hummingbot.connector.in_flight_order_base import InFlightOrderBase
from hummingbot.core.event.events import (OrderFilledEvent, MarketEvent)

import logging

cdef class FtxPerpInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 leverage: int,
                 position: str,
                 created_at: float,
                 initial_state: str = "new"):
        super().__init__(client_order_id=client_order_id,
                         exchange_order_id=exchange_order_id,
                         trading_pair=trading_pair,
                         order_type=order_type,
                         trade_type=trade_type,
                         price=price,
                         amount=amount,
                         initial_state=initial_state,
                         creation_timestamp=created_at)
        self.created_at = created_at
        self.state = FtxPerpOrderStatus.new
        self.leverage = leverage
        self.position = position

    def __repr__(self) -> str:
        return f"super().__repr__()" \
               f"created_at='{str(self.created_at)}'')"

    def to_json(self) -> Dict[str, Any]:
        response = super().to_json()
        response["created_at"] = str(self.created_at)
        return response
    
    def get_position_action(self, trade_type):
        return 

    @property
    def is_done(self) -> bool:
        return self.state is FtxPerpOrderStatus.closed

    @property
    def is_failure(self) -> bool:
        return self.state is FtxPerpOrderStatus.FAILURE or self.is_cancelled

    @property
    def is_cancelled(self) -> bool:
        return self.state is FtxPerpOrderStatus.closed and self.executed_amount_base < self.amount

    def set_status(self, status: str):
        self.last_state = status
        self.state = FtxPerpOrderStatus[status]

    @property
    def order_type_description(self) -> str:
        order_type = "market" if self.order_type is OrderType.MARKET else "limit"
        side = "buy" if self.trade_type is TradeType.BUY else "sell"
        return f"{order_type} {side}"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        cdef:
            FtxPerpInFlightOrder retval = FtxPerpInFlightOrder(
                client_order_id=data["client_order_id"],
                exchange_order_id=data["exchange_order_id"],
                trading_pair=data["trading_pair"],
                order_type=getattr(OrderType, data["order_type"]),
                trade_type=getattr(TradeType, data["trade_type"]),
                price=Decimal(data["price"]),
                amount=Decimal(data["amount"]),
                leverage=data["leverage"] if "leverage" in data else 1,
                position=(data["position"] 
                          if "position" in data 
                          else (PositionAction.OPEN.name if getattr(TradeType, data["trade_type"]) is TradeType.BUY else PositionAction.CLOSE.name)),
                created_at=float(data["created_at"] if "created_at" in data else 0),
                initial_state=data["last_state"]
            )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        retval.state = FtxPerpOrderStatus[retval.last_state]
        return retval

    def update(self, data: Dict[str, Any]) -> List[Any]:
        events: List[Any] = []

        new_status: FtxPerpOrderStatus = FtxPerpOrderStatus[data["status"]]
        old_executed_base: Decimal = self.executed_amount_base
        old_executed_quote: Decimal = self.executed_amount_quote
        overall_executed_base: Decimal = data["filledSize"]
        overall_remaining_size: Decimal = self.amount - overall_executed_base
        if data["avgFillPrice"] is not None:
            overall_executed_quote: Decimal = overall_executed_base * data["avgFillPrice"]
        else:
            overall_executed_quote: Decimal = Decimal("0")

        diff_base: Decimal = overall_executed_base - old_executed_base
        diff_quote: Decimal = overall_executed_quote - old_executed_quote

        if diff_base > 0:
            diff_price: Decimal = diff_quote / diff_base
            events.append((MarketEvent.OrderFilled, diff_base, diff_price, None))

        if not self.is_done and new_status == FtxPerpOrderStatus.closed:
            if overall_remaining_size > 0:
                events.append((MarketEvent.OrderCancelled, None, None, None))
            elif self.trade_type is TradeType.BUY:
                events.append((MarketEvent.BuyOrderCompleted, overall_executed_base, overall_executed_quote, None))
            else:
                events.append((MarketEvent.SellOrderCompleted, overall_executed_base, overall_executed_quote, None))

        self.state = new_status
        self.last_state = new_status.name
        self.executed_amount_base = overall_executed_base
        self.executed_amount_quote = overall_executed_quote

        return events

    def update_fees(self, new_fee: Decimal):
        self.fee_paid += new_fee
