import asyncio
import logging
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np

from hummingbot.connector.budget_checker import OrderCandidate
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.derivative.perpetual_budget_checker import PerpetualOrderCandidate
from hummingbot.connector.derivative.position import Position
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    OrderType,
    PositionAction,
    PositionMode,
    SellOrderCompletedEvent,
    TradeType
)
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from ..__utils__.linear_regression import LinearRegression

from .arb_proposal import ArbProposal, ArbProposalSide

NaN = float("nan")
s_decimal_zero = Decimal(0)
spa_logger = None


class StrategyState(Enum):
    Closed = 0
    Opening = 1
    Opened = 2
    Closing = 3


class SpotPerpetualArbitrageStrategy(StrategyPyBase):
    """
    This strategy arbitrages between a spot and a perpetual exchange.
    For a given order amount, the strategy checks for price discrepancy between buy and sell price on the 2 exchanges.
    Since perpetual contract requires closing position before profit is realised, there are 2 stages to this arbitrage
    operation - first to open and second to close.
    """

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global spa_logger
        if spa_logger is None:
            spa_logger = logging.getLogger(__name__)
        return spa_logger

    def init_params(self,
                    spot_market_info: MarketTradingPairTuple,
                    perp_market_info: MarketTradingPairTuple,
                    order_amount: Decimal,
                    perp_leverage: int,
                    min_opening_arbitrage_pct: Decimal,
                    min_closing_arbitrage_pct: Decimal,
                    spot_market_slippage_buffer: Decimal = Decimal("0"),
                    perp_market_slippage_buffer: Decimal = Decimal("0"),
                    next_arbitrage_opening_delay: float = 120,
                    use_oracle_conversion_rate: bool = False,
                    perp_to_spot_base_conversion_rate: Decimal = Decimal("1"),
                    perp_to_spot_quote_conversion_rate: Decimal = Decimal("1"),
                    reg_buffer_size: int = 288,
                    min_buffer_sample: int = 144,
                    enable_reg_offset: bool = False,
                    fixed_beta: Decimal = Decimal("0.02"),
                    status_report_interval: float = 10):
        """
        :param spot_market_info: The spot market info
        :param perp_market_info: The perpetual market info
        :param order_amount: The order amount
        :param perp_leverage: The leverage level to use on perpetual market
        :param min_opening_arbitrage_pct: The minimum spread to open arbitrage position (e.g. 0.0003 for 0.3%)
        :param min_closing_arbitrage_pct: The minimum spread to close arbitrage position (e.g. 0.0003 for 0.3%)
        :param spot_market_slippage_buffer: The buffer for which to adjust order price for higher chance of
        the order getting filled on spot market.
        :param perp_market_slippage_buffer: The slipper buffer for perpetual market.
        :param next_arbitrage_opening_delay: The number of seconds to delay before the next arb position can be opened
        :param status_report_interval: Amount of seconds to wait to refresh the status report
        """
        self._spot_market_info = spot_market_info
        self._perp_market_info = perp_market_info
        self._min_opening_arbitrage_pct = min_opening_arbitrage_pct
        self._min_closing_arbitrage_pct = min_closing_arbitrage_pct
        self._order_amount = order_amount
        self._perp_leverage = perp_leverage
        self._spot_market_slippage_buffer = spot_market_slippage_buffer
        self._perp_market_slippage_buffer = perp_market_slippage_buffer
        self._next_arbitrage_opening_delay = next_arbitrage_opening_delay
        self._next_arbitrage_opening_ts = 0  # next arbitrage opening timestamp
        self._all_markets_ready = False
        self._ev_loop = asyncio.get_event_loop()
        self._last_timestamp = 0
        self._status_report_interval = status_report_interval
        self.add_markets([spot_market_info.market, perp_market_info.market])

        self._main_task = None
        self._in_flight_opening_order_ids = []
        self._completed_opening_order_ids = []
        self._completed_closing_order_ids = []
        self._strategy_state = StrategyState.Closed
        self._ready_to_start = False
        self._last_arb_op_reported_ts = 0
        perp_market_info.market.set_leverage(perp_market_info.trading_pair, self._perp_leverage)

        self._use_oracle_conversion_rate = use_oracle_conversion_rate
        self._perp_to_spot_base_conversion_rate = perp_to_spot_base_conversion_rate
        self._perp_to_spot_quote_conversion_rate = perp_to_spot_quote_conversion_rate
        self._reg_buffer_size = reg_buffer_size
        self._min_buffer_sample = min_buffer_sample
        self._enable_reg_offset = enable_reg_offset
        self._fixed_beta = fixed_beta
        self._lin_reg = LinearRegression(sampling_length=reg_buffer_size)

    @property
    def strategy_state(self) -> StrategyState:
        return self._strategy_state

    @property
    def min_opening_arbitrage_pct(self) -> Decimal:
        return self._min_opening_arbitrage_pct

    @property
    def min_closing_arbitrage_pct(self) -> Decimal:
        return self._min_closing_arbitrage_pct

    @property
    def order_amount(self) -> Decimal:
        return self._order_amount

    @order_amount.setter
    def order_amount(self, value):
        self._order_amount = value

    @property
    def reg_buffer_size(self) -> Decimal:
        return self._reg_buffer_size

    @reg_buffer_size.setter
    def reg_buffer_size(self, value):
        self._reg_buffer_size = value
    
    @property
    def min_buffer_sample(self) -> Decimal:
        return self._min_buffer_sample

    @min_buffer_sample.setter
    def min_buffer_sample(self, value):
        self._min_buffer_sample = value

    @property
    def enable_reg_offset(self) -> Decimal:
        return self._enable_reg_offset

    @property
    def fixed_beta(self) -> Decimal:
        return self._fixed_beta

    @fixed_beta.setter
    def fixed_beta(self, value):
        self._fixed_beta = value
    
    @property
    def lin_reg(self):
        return self._lin_reg

    @lin_reg.setter
    def lin_reg(self, indicator: LinearRegression):
        self._lin_reg = indicator

    @property
    def market_info_to_active_orders(self) -> Dict[MarketTradingPairTuple, List[LimitOrder]]:
        return self._sb_order_tracker.market_pair_to_active_orders

    @property
    def perp_positions(self) -> List[Position]:
        return [s for s in self._perp_market_info.market.account_positions.values() if
                s.trading_pair == self._perp_market_info.trading_pair and s.amount != s_decimal_zero]

    def get_perp_to_spot_conversion_rate(self) -> Tuple[str, Decimal, str, Decimal]:
        """
        Find conversion rates from spot market to perp market
        :return: A tuple of quote pair symbol, quote conversion rate source, quote conversion rate,
        base pair symbol, base conversion rate source, base conversion rate
        """
        quote_rate = Decimal("1")
        quote_pair = f"{self._perp_market_info.quote_asset}-{self._spot_market_info.quote_asset}"
        quote_rate_source = "fixed"
        if self._use_oracle_conversion_rate:
            if self._perp_market_info.quote_asset != self._spot_market_info.quote_asset:
                quote_rate_source = RateOracle.source.name
                quote_rate = RateOracle.get_instance().rate(quote_pair)
        else:
            quote_rate = self._perp_to_spot_quote_conversion_rate
        base_rate = Decimal("1")
        base_pair = f"{self._perp_market_info.base_asset}-{self._spot_market_info.base_asset}"
        base_rate_source = "fixed"
        if self._use_oracle_conversion_rate:
            if self._perp_market_info.base_asset != self._spot_market_info.base_asset:
                base_rate_source = RateOracle.source.name
                base_rate = RateOracle.get_instance().rate(base_pair)
        else:
            base_rate = self._perp_to_spot_base_conversion_rate
        return quote_pair, quote_rate_source, quote_rate, base_pair, base_rate_source, base_rate

    def market_conversion_rate(self, is_adjusted) -> Decimal:
        """
        Return price conversion rate for a taker market (to convert it into maker base asset value)
        """
        _, _, quote_rate, _, _, base_rate = self.get_perp_to_spot_conversion_rate()
        avg_premium = Decimal("1")
        if is_adjusted and self._enable_reg_offset:
            avg_premium = Decimal(str(self._lin_reg.beta)) if self._lin_reg.samples_filled() > self._min_buffer_sample else (Decimal("1") + self._fixed_beta)
        return (quote_rate / base_rate) * avg_premium

    def get_average_premium(self):
        if self._enable_reg_offset and self._lin_reg.samples_filled() > self._min_buffer_sample:
            return Decimal(str(self._lin_reg.premium))
        else:
            return self._fixed_beta

    def get_min_profitability(self):
        reg_margin = Decimal(str(self._lin_reg.mean + (1.5 * self._lin_reg.std)))
        return reg_margin if reg_margin > self._min_profitability else self._min_profitability

    def tick(self, timestamp: float):
        """
        Clock tick entry point, is run every second (on normal tick setting).
        :param timestamp: current tick timestamp
        """
        if not self._all_markets_ready:
            self._all_markets_ready = all([market.ready for market in self.active_markets])
            if not self._all_markets_ready:
                return
            else:
                self.logger().info("Markets are ready.")
                self.logger().info("Trading started.")

                if not self.check_budget_available():
                    self.logger().info("Trading not possible.")
                    return

                if self._perp_market_info.market.position_mode != PositionMode.ONEWAY or \
                        len(self.perp_positions) > 1:
                    self.logger().info("This strategy supports only Oneway position mode. Please update your position "
                                       "mode before starting this strategy.")
                    return

                if len(self.perp_positions) == 1:
                    self._strategy_state = StrategyState.Opened
                    self._ready_to_start = True
                    # adj_perp_amount = self._perp_market_info.market.quantize_order_amount(
                    #     self._perp_market_info.trading_pair, self._order_amount)
                    # if abs(self.perp_positions[0].amount) == adj_perp_amount:
                    #     self.logger().info(f"There is an existing {self._perp_market_info.trading_pair} "
                    #                        f"{self.perp_positions[0].position_side.name} position. The bot resumes "
                    #                        f"operation to close out the arbitrage position")
                    #     self._strategy_state = StrategyState.Opened
                    #     self._ready_to_start = True
                    # else:
                    #     self.logger().info(f"There is an existing {self._perp_market_info.trading_pair} "
                    #                        f"{self.perp_positions[0].position_side.name} position with unmatched "
                    #                        f"position amount. Please manually close out the position before starting "
                    #                        f"this strategy.")
                    #     return
                else:
                    self._ready_to_start = True
        else:
            if self._last_arb_op_reported_ts + (60 * 5) < self.current_timestamp:
                spot_bid = self._spot_market_info.market.get_price(self._spot_market_info.trading_pair, False)
                spot_ask = self._spot_market_info.market.get_price(self._spot_market_info.trading_pair, True)
                perp_bid = self._perp_market_info.market.get_price(self._perp_market_info.trading_pair, False) * self.market_conversion_rate(False)
                perp_ask = self._perp_market_info.market.get_price(self._perp_market_info.trading_pair, True) * self.market_conversion_rate(False)
                # m_bid, m_ask, t_bid, t_ask = self.c_get_bid_ask_prices(market_pair, False)
                sample = {
                    "ts": self.current_timestamp,
                    "y": {"bid": spot_bid, "ask": spot_ask},
                    "x": {"bid": perp_bid, "ask": perp_ask}
                }
                self._lin_reg.add_sample(sample)

        if self._ready_to_start and (self._main_task is None or self._main_task.done()):
            self._main_task = safe_ensure_future(self.main(timestamp))

    async def main(self, timestamp):
        """
        The main procedure for the arbitrage strategy.
        """
        self.update_strategy_state()
        if self._strategy_state in (StrategyState.Opening, StrategyState.Closing):
            return
        if self.strategy_state == StrategyState.Opened and self._next_arbitrage_opening_ts > self.current_timestamp:
            return
        proposals = await self.create_base_proposals()
        conversion_rate = self.market_conversion_rate(True)

        close_proposals = []
        if self._strategy_state == StrategyState.Opened:
            perp_is_buy = False if self.perp_positions[0].amount > 0 else True
            close_proposals = [p for p in proposals if p.perp_side.is_buy == perp_is_buy and p.profit_pct(conversion_rate) >=
                         self.get_min_profitability()]
        
        open_proposals = [p for p in proposals if p.profit_pct(conversion_rate) >= self.get_min_profitability()]
        proposals = close_proposals + open_proposals

        if len(proposals) == 0:
            return
        proposal = proposals[0]
        if self._last_arb_op_reported_ts + 60 < self.current_timestamp:
            is_closing = True if proposals == close_proposals else False
            pos_txt = "closing" if is_closing else "opening"
            # pos_txt = "closing" if self._strategy_state == StrategyState.Opened else "opening"
            self.logger().info(f"Arbitrage position {pos_txt} opportunity found.")
            self.logger().info(f"Profitability ({proposal.profit_pct(conversion_rate):.2%}) is now above min_{pos_txt}_arbitrage_pct.")
            self._last_arb_op_reported_ts = self.current_timestamp
        self.apply_slippage_buffers(proposal)
        if self.check_budget_constraint(proposal):
            self.execute_arb_proposal(proposal)

    def update_strategy_state(self):
        """
        Updates strategy state to either Opened or Closed if the condition is right.
        """
        if self._strategy_state == StrategyState.Opening and len(self._completed_opening_order_ids) == 2 and \
                self.perp_positions:
            self._strategy_state = StrategyState.Opened
            self._completed_opening_order_ids.clear()
            self._next_arbitrage_opening_ts = self.current_timestamp + self._next_arbitrage_opening_delay
        elif self._strategy_state == StrategyState.Closing and len(self._completed_closing_order_ids) == 2 and \
                len(self.perp_positions) == 0:
            self._strategy_state = StrategyState.Closed
            self._completed_closing_order_ids.clear()
            # self._next_arbitrage_opening_ts = self.current_timestamp + self._next_arbitrage_opening_delay

    async def create_base_proposals(self) -> List[ArbProposal]:
        """
        Creates a list of 2 base proposals, no filter.
        :return: A list of 2 base proposals.
        """
        tasks = [self._spot_market_info.market.get_order_price(self._spot_market_info.trading_pair, True,
                                                               self._order_amount),
                 self._spot_market_info.market.get_order_price(self._spot_market_info.trading_pair, False,
                                                               self._order_amount),
                 self._perp_market_info.market.get_order_price(self._perp_market_info.trading_pair, True,
                                                               self._order_amount),
                 self._perp_market_info.market.get_order_price(self._perp_market_info.trading_pair, False,
                                                               self._order_amount)]
        prices = await safe_gather(*tasks, return_exceptions=True)
        spot_buy, spot_sell, perp_buy, perp_sell = [*prices]
        return [
            ArbProposal(ArbProposalSide(self._spot_market_info, True, spot_buy),
                        ArbProposalSide(self._perp_market_info, False, perp_sell),
                        self._order_amount),
            ArbProposal(ArbProposalSide(self._spot_market_info, False, spot_sell),
                        ArbProposalSide(self._perp_market_info, True, perp_buy),
                        self._order_amount)
        ]

    def apply_slippage_buffers(self, proposal: ArbProposal):
        """
        Updates arb_proposals by adjusting order price for slipper buffer percentage.
        E.g. if it is a buy order, for an order price of 100 and 1% slipper buffer, the new order price is 101,
        for a sell order, the new order price is 99.
        :param proposal: the arbitrage proposal
        """
        for arb_side in (proposal.spot_side, proposal.perp_side):
            market = arb_side.market_info.market
            # arb_side.amount = market.quantize_order_amount(arb_side.market_info.trading_pair, arb_side.amount)
            s_buffer = self._spot_market_slippage_buffer if market == self._spot_market_info.market \
                else self._perp_market_slippage_buffer
            if not arb_side.is_buy:
                s_buffer *= Decimal("-1")
            arb_side.order_price *= Decimal("1") + s_buffer
            arb_side.order_price = market.quantize_order_price(arb_side.market_info.trading_pair,
                                                               arb_side.order_price)

    def check_budget_available(self) -> bool:
        """
        Checks if there's any balance for trading to be possible at all
        :return: True if user has available balance enough for orders submission.
        """

        spot_base, spot_quote = self._spot_market_info.trading_pair.split("-")
        perp_base, perp_quote = self._perp_market_info.trading_pair.split("-")

        balance_spot_base = self._spot_market_info.market.get_available_balance(spot_base)
        balance_spot_quote = self._spot_market_info.market.get_available_balance(spot_quote)

        balance_perp_quote = self._perp_market_info.market.get_available_balance(perp_quote)

        if balance_spot_base == s_decimal_zero and balance_spot_quote == s_decimal_zero:
            self.logger().info(f"Cannot arbitrage, {self._spot_market_info.market.display_name} {spot_base} balance "
                               f"({balance_spot_base}) is 0 and {self._spot_market_info.market.display_name} {spot_quote} balance "
                               f"({balance_spot_quote}) is 0.")
            return False

        if balance_perp_quote == s_decimal_zero:
            self.logger().info(f"Cannot arbitrage, {self._perp_market_info.market.display_name} {perp_quote} balance "
                               f"({balance_perp_quote}) is 0.")
            return False

        return True

    def check_budget_constraint(self, proposal: ArbProposal) -> bool:
        """
        Check balances on both exchanges if there is enough to submit both orders in a proposal.
        :param proposal: An arbitrage proposal
        :return: True if user has available balance enough for both orders submission.
        """
        return self.check_spot_budget_constraint(proposal) and self.check_perpetual_budget_constraint(proposal)

    def check_spot_budget_constraint(self, proposal: ArbProposal) -> bool:
        """
        Check balance on spot exchange.
        :param proposal: An arbitrage proposal
        :return: True if user has available balance enough for both orders submission.
        """
        proposal_side = proposal.spot_side
        order_amount = proposal.order_amount
        market_info = proposal_side.market_info
        budget_checker = market_info.market.budget_checker
        order_candidate = OrderCandidate(
            trading_pair=market_info.trading_pair,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY if proposal_side.is_buy else TradeType.SELL,
            amount=order_amount,
            price=proposal_side.order_price,
        )

        adjusted_candidate_order = budget_checker.adjust_candidate(order_candidate, all_or_none=True)

        if adjusted_candidate_order.amount < order_amount:
            # self.logger().info(
            #     f"Cannot arbitrage, {proposal_side.market_info.market.display_name}"
            #     f" {adjusted_candidate_order.collateral_token} balance ({adjusted_candidate_order.collateral_amount})"
            #     f" is below required to place the order amount {order_amount}."
            # )
            return False

        return True

    def check_perpetual_budget_constraint(self, proposal: ArbProposal) -> bool:
        """
        Check balance on spot exchange.
        :param proposal: An arbitrage proposal
        :return: True if user has available balance enough for both orders submission.
        """
        proposal_side = proposal.perp_side
        order_amount = proposal.order_amount
        market_info = proposal_side.market_info
        budget_checker = market_info.market.budget_checker

        position_close = False
        # if self.perp_positions and abs(self.perp_positions[0].amount) == order_amount:
        if self.perp_positions:
            perp_side = proposal.perp_side
            cur_perp_pos_is_buy = True if self.perp_positions[0].amount > 0 else False
            if perp_side != cur_perp_pos_is_buy:
                position_close = True

        order_candidate = PerpetualOrderCandidate(
            trading_pair=market_info.trading_pair,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY if proposal_side.is_buy else TradeType.SELL,
            amount=order_amount,
            price=proposal_side.order_price,
            leverage=Decimal(self._perp_leverage),
            position_close=position_close,
        )

        adjusted_candidate_order = budget_checker.adjust_candidate(order_candidate, all_or_none=True)

        if adjusted_candidate_order.amount < order_amount:
            # self.logger().info(
            #     f"Cannot arbitrage, {proposal_side.market_info.market.display_name}"
            #     f" {adjusted_candidate_order.collateral_token} balance ({adjusted_candidate_order.collateral_amount})"
            #     f" is below required to place the order amount {order_amount}."
            # )
            return False

        return True

    def execute_arb_proposal(self, proposal: ArbProposal):
        """
        Execute both sides of the arbitrage trades concurrently.
        :param proposal: the arbitrage proposal
        """
        conversion_rate = self.market_conversion_rate(True)
        if proposal.order_amount == s_decimal_zero:
            return
        spot_side = proposal.spot_side
        spot_order_fn = self.buy_with_specific_market if spot_side.is_buy else self.sell_with_specific_market
        spot_action = "BUY" if spot_side.is_buy else "SELL"
        # self.log_with_clock(
        #     logging.INFO,
        #     f"Placing {spot_action} order for {proposal.order_amount} {spot_side.market_info.base_asset} "
        #     f"at {spot_side.market_info.market.display_name} at {spot_side.order_price} price"
        # )
        spot_order_fn(
            spot_side.market_info,
            proposal.order_amount,
            spot_side.market_info.market.get_taker_order_type(),
            spot_side.order_price,
        )
        perp_side = proposal.perp_side
        perp_order_fn = self.buy_with_specific_market if perp_side.is_buy else self.sell_with_specific_market
        perp_action = "BUY" if perp_side.is_buy else "SELL"
        
        position_action = PositionAction.CLOSE if proposal.profit_pct(conversion_rate) >= self.get_min_profitability() else PositionAction.OPEN
        # position_action = PositionAction.CLOSE if self._strategy_state == StrategyState.Opened else PositionAction.OPEN
        # self.log_with_clock(
        #     logging.INFO,
        #     f"Placing {perp_action} order for {proposal.order_amount} {perp_side.market_info.base_asset} "
        #     f"at {perp_side.market_info.market.display_name} at {perp_side.order_price} price to "
        #     f"{position_action.name} position."
        # )
        perp_order_fn(
            perp_side.market_info,
            proposal.order_amount,
            perp_side.market_info.market.get_taker_order_type(),
            perp_side.order_price,
            position_action=position_action
        )
        # if self._strategy_state == StrategyState.Opened:
        #     self._strategy_state = StrategyState.Closing
        #     self._completed_closing_order_ids.clear()
        # else:
        #     self._strategy_state = StrategyState.Opening
        #     self._completed_opening_order_ids.clear()

        self.log_with_clock(
            logging.INFO,
            f"[Order] {spot_action}-{proposal.order_amount}={spot_side.market_info.base_asset}-"
            f"-{spot_side.market_info.market.display_name}-{spot_side.order_price}"
            f"-{perp_action}-{proposal.order_amount}-{perp_side.market_info.base_asset}"
            f"-{perp_side.market_info.market.display_name}-{perp_side.order_price}"
        )
        sell_price = spot_side.order_price if spot_action == "SELL" else (perp_side.order_price * self.market_conversion_rate(False))
        buy_price = spot_side.order_price if spot_action == "BUY" else (perp_side.order_price * self.market_conversion_rate(False))
        profit = (sell_price / buy_price) - 1

        self.log_with_clock(
            logging.INFO,
            f"[Arb] {profit:.2%} {spot_action} {spot_side.market_info.market.display_name} "
            f"{perp_action} {perp_side.market_info.market.display_name}"
        )

        if self._strategy_state in (StrategyState.Opened, StrategyState.Closed):
            self._strategy_state = StrategyState.Opening
            self._completed_opening_order_ids.clear()

    def active_positions_df(self) -> pd.DataFrame:
        """
        Returns a new dataframe on current active perpetual positions.
        """
        columns = ["Symbol", "Type", "Entry Price", "Amount", "Leverage", "Unrealized PnL"]
        data = []
        for pos in self.perp_positions:
            data.append([
                pos.trading_pair,
                "LONG" if pos.amount > 0 else "SHORT",
                pos.entry_price,
                pos.amount,
                pos.leverage,
                pos.unrealized_pnl
            ])

        return pd.DataFrame(data=data, columns=columns)

    async def format_status(self) -> str:
        """
        Returns a status string formatted to display nicely on terminal. The strings composes of 4 parts: markets,
        assets, spread and warnings(if any).
        """
        columns = ["Exchange", "Market", "Sell Price", "Buy Price", "Mid Price"]
        data = []
        spot = self._spot_market_info
        perp = self._perp_market_info

        for market_info in [spot, perp]:
            market, trading_pair, base_asset, quote_asset = market_info
            buy_price = await market.get_quote_price(trading_pair, True, self._order_amount)
            sell_price = await market.get_quote_price(trading_pair, False, self._order_amount)
            mid_price = (buy_price + sell_price) / 2
            data.append([
                market.display_name,
                trading_pair,
                float(sell_price),
                float(buy_price),
                float(mid_price)
            ])
        markets_df = pd.DataFrame(data=data, columns=columns)
        lines = []
        lines.extend(["", "  Markets:"] + ["    " + line for line in markets_df.to_string(index=False).split("\n")])

        # See if there're any active positions.
        if len(self.perp_positions) > 0:
            df = self.active_positions_df()
            lines.extend(["", "  Positions:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        else:
            lines.extend(["", "  No active positions."])

        assets_df = self.wallet_balance_data_frame([spot, perp])
        quote_asset = perp.quote_asset
        base_asset = perp.base_asset
        total_base_asset = assets_df.loc[assets_df["Asset"] == base_asset]["Total Balance"].sum()
        quote_df = assets_df.loc[assets_df["Asset"] != base_asset]
        total_quote_asset = np.where(quote_df["Asset"] == quote_asset, quote_df["Total Balance"], quote_df["Total Balance"] / float(self.market_conversion_rate(False))).sum()
        lines.extend(["", f"  Assets: {total_base_asset:.2f}{base_asset} {total_quote_asset:.2f}{quote_asset}"] +
                     ["    " + line for line in str(assets_df).split("\n")])

        # proposals = await self.create_base_proposals()
        spot_buy = spot.market.get_price(spot.trading_pair, True)
        spot_sell = spot.market.get_price(spot.trading_pair, False)
        perp_buy = perp.market.get_price(perp.trading_pair, True) * self.market_conversion_rate(False)
        perp_sell = perp.market.get_price(perp.trading_pair, False) * self.market_conversion_rate(False)
        perp_buy_adj = perp.market.get_price(perp.trading_pair, True) * self.market_conversion_rate(True)
        perp_sell_adj = perp.market.get_price(perp.trading_pair, False) * self.market_conversion_rate(True)

        op_lines = []
        op_lines.append(f"{'    '}buy at {spot.market.display_name}"
                         f", sell at {perp.market.display_name}: "
                         f"{(perp_sell / spot_buy - 1):.2%} Adj {(perp_sell_adj/spot_buy - 1):.2%}")
        op_lines.append(f"{'    '}sell at {spot.market.display_name}"
                         f", buy at {perp.market.display_name}: "
                         f"{(spot_sell / perp_buy - 1):.2%} Adj {(spot_sell / perp_buy_adj - 1):.2%}")

        lines.extend(["", f"  Opportunity: Prem {self.get_average_premium():.2%}, Std {self._lin_reg.std:.2%}"] + op_lines)

        # current conversion rate, premium, 

        warning_lines = self.network_warning([spot])
        warning_lines.extend(self.network_warning([perp]))
        warning_lines.extend(self.balance_warning([spot]))
        warning_lines.extend(self.balance_warning([perp]))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def short_proposal_msg(self, arb_proposal: List[ArbProposal], indented: bool = True) -> List[str]:
        """
        Composes a short proposal message.
        :param arb_proposal: The arbitrage proposal
        :param indented: If the message should be indented (by 4 spaces)
        :return A list of messages
        """
        lines = []
        for proposal in arb_proposal:
            spot_side = "buy" if proposal.spot_side.is_buy else "sell"
            perp_side = "buy" if proposal.perp_side.is_buy else "sell"
            profit_pct = proposal.profit_pct(self.market_conversion_rate(False))
            adj_profit_pct = proposal.profit_pct(self.market_conversion_rate(True))
            lines.append(f"{'    ' if indented else ''}{spot_side} at "
                         f"{proposal.spot_side.market_info.market.display_name}"
                         f", {perp_side} at {proposal.perp_side.market_info.market.display_name}: "
                         f"{profit_pct:.2%} Adj {adj_profit_pct:.2%}")
        return lines

    @property
    def tracked_market_orders(self) -> List[Tuple[ConnectorBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_market_orders

    @property
    def tracked_limit_orders(self) -> List[Tuple[ConnectorBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    def start(self, clock: Clock, timestamp: float):
        self._ready_to_start = False

    def stop(self, clock: Clock):
        if self._main_task is not None:
            self._main_task.cancel()
            self._main_task = None
        self._ready_to_start = False

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        self.update_complete_order_id_lists(event.order_id)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        self.update_complete_order_id_lists(event.order_id)

    def update_complete_order_id_lists(self, order_id: str):
        if self._strategy_state == StrategyState.Opening:
            self._completed_opening_order_ids.append(order_id)
        elif self._strategy_state == StrategyState.Closing:
            self._completed_closing_order_ids.append(order_id)
