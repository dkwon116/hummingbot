from decimal import Decimal
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.spot_perpetual_arbitrage.spot_perpetual_arbitrage import SpotPerpetualArbitrageStrategy
from hummingbot.strategy.spot_perpetual_arbitrage.spot_perpetual_arbitrage_config_map import spot_perpetual_arbitrage_config_map


def start(self):
    spot_connector = spot_perpetual_arbitrage_config_map.get("spot_connector").value.lower()
    spot_market = spot_perpetual_arbitrage_config_map.get("spot_market").value
    perpetual_connector = spot_perpetual_arbitrage_config_map.get("perpetual_connector").value.lower()
    perpetual_market = spot_perpetual_arbitrage_config_map.get("perpetual_market").value
    order_amount = spot_perpetual_arbitrage_config_map.get("order_amount").value
    perpetual_leverage = spot_perpetual_arbitrage_config_map.get("perpetual_leverage").value
    min_opening_arbitrage_pct = spot_perpetual_arbitrage_config_map.get("min_opening_arbitrage_pct").value / Decimal("100")
    min_closing_arbitrage_pct = spot_perpetual_arbitrage_config_map.get("min_closing_arbitrage_pct").value / Decimal("100")
    spot_market_slippage_buffer = spot_perpetual_arbitrage_config_map.get("spot_market_slippage_buffer").value / Decimal("100")
    perpetual_market_slippage_buffer = spot_perpetual_arbitrage_config_map.get("perpetual_market_slippage_buffer").value / Decimal("100")
    next_arbitrage_opening_delay = spot_perpetual_arbitrage_config_map.get("next_arbitrage_opening_delay").value

    use_oracle_conversion_rate = spot_perpetual_arbitrage_config_map.get("use_oracle_conversion_rate").value
    perp_to_spot_base_conversion_rate = spot_perpetual_arbitrage_config_map.get("perp_to_spot_base_conversion_rate").value
    perp_to_spot_quote_conversion_rate = spot_perpetual_arbitrage_config_map.get("perp_to_spot_quote_conversion_rate").value
    reg_buffer_size = spot_perpetual_arbitrage_config_map["reg_buffer_size"].value
    min_buffer_sample = spot_perpetual_arbitrage_config_map["min_buffer_sample"].value
    enable_reg_offset = spot_perpetual_arbitrage_config_map["enable_reg_offset"].value
    fixed_beta = spot_perpetual_arbitrage_config_map.get("fixed_beta").value / Decimal("100")

    self._initialize_markets([(spot_connector, [spot_market]), (perpetual_connector, [perpetual_market])])
    base_1, quote_1 = spot_market.split("-")
    base_2, quote_2 = perpetual_market.split("-")

    spot_market_info = MarketTradingPairTuple(self.markets[spot_connector], spot_market, base_1, quote_1)
    perpetual_market_info = MarketTradingPairTuple(self.markets[perpetual_connector], perpetual_market, base_2, quote_2)

    self.market_trading_pair_tuples = [spot_market_info, perpetual_market_info]
    self.strategy = SpotPerpetualArbitrageStrategy()
    self.strategy.init_params(spot_market_info,
                              perpetual_market_info,
                              order_amount,
                              perpetual_leverage,
                              min_opening_arbitrage_pct,
                              min_closing_arbitrage_pct,
                              spot_market_slippage_buffer,
                              perpetual_market_slippage_buffer,
                              next_arbitrage_opening_delay,
                              use_oracle_conversion_rate,
                              perp_to_spot_base_conversion_rate,
                              perp_to_spot_quote_conversion_rate,
                              reg_buffer_size,
                              min_buffer_sample,
                              enable_reg_offset,
                              fixed_beta)
