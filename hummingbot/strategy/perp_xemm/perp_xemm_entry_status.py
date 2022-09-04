#!/usr/bin/env python
from decimal import Decimal
from re import A

from regex import B
from sympy import C

s_decimal_zero = Decimal("0")
s_decimal_nan = Decimal("nan")
zero_tuple = (s_decimal_zero, s_decimal_zero)


class PerpXEMMEntryStatus:
    """
    
    """
    def __init__(self) -> None:
        self._current_entry_level = 0
        self._current_entry_ratio = s_decimal_zero
        self._average_ratio = s_decimal_zero
        self._maker_entry = zero_tuple
        self._taker_entry = zero_tuple
        self._maker_volume = s_decimal_zero
        
        self._trade_in_progress = False
        self._balancing_active = False
        self._update_ratio = False
        self._set_bo_ratio = False
        self._maker_sold_in_progress = False

    @property
    def current_entry_level(self):
        return self._current_entry_level
    
    @current_entry_level.setter
    def current_entry_level(self, value):
        self._current_entry_level = value

    @property
    def current_entry_ratio(self):
        return self._current_entry_ratio
    
    @current_entry_ratio.setter
    def current_entry_ratio(self, value):
        self._current_entry_ratio = value
    
    @property
    def average_ratio(self):
        return self._average_ratio
    
    @average_ratio.setter
    def average_ratio(self, value):
        self._average_ratio = value

    @property
    def maker_entry(self):
        return self._maker_entry
    
    @maker_entry.setter
    def maker_entry(self, value):
        self._maker_entry = value

    @property
    def taker_entry(self):
        return self._taker_entry
    
    @taker_entry.setter
    def taker_entry(self, value):
        self._taker_entry = value
    
    @property
    def maker_volume(self):
        return self._maker_volume
    
    @maker_volume.setter
    def maker_volume(self, value):
        self._maker_volume = value

    @property
    def trade_in_progress(self):
        return self._trade_in_progress
    
    @maker_entry.setter
    def maker_entry(self, value):
        self._maker_entry = value

    @property
    def balancing_active(self):
        return self._balancing_active

    @property
    def update_ratio(self):
        return self._update_ratio

    @property
    def set_bo_ratio(self):
        return self._set_bo_ratio
    
    @property
    def maker_sold_in_progress(self):
        return self._maker_sold_in_progress

    def reset_entry_status(self):
        self._current_entry_level = 0
        self._current_entry_ratio = s_decimal_zero
        self._average_ratio = s_decimal_zero
        self._maker_entry = zero_tuple
        self._taker_entry = zero_tuple
        self._trade_in_progress = False
        self._balancing_active = False
        self._update_ratio = False
        self._maker_sold_in_progress = False

    def get_entry_value(self, market, value_type):
        entry = self._maker_entry if market == "maker" else self._taker_entry

        if value_type == "quote":
            return entry[0]
        elif value_type == "base":
            return entry[1]
        else:
            return s_decimal_nan

    def maker_sold(self, filled_quote, filled_base):
        self._maker_entry = (self._maker_entry[0] + filled_quote, self._maker_entry[1] + filled_base)
        self._trade_in_progress = True
        self._balancing_active = True
        self._update_ratio = True
        self._maker_sold_in_progress = True

    def taker_order_filled(self, filled_quote, filled_base, is_buy):
        quote = filled_quote if is_buy else -filled_quote
        base = filled_base if is_buy else -filled_base
        self._taker_entry = (self._taker_entry[0] + quote, self._taker_entry[1] + base)

    def update_entry_level(self, new_level, new_ratio):
        self._current_entry_level = new_level
        self._current_entry_ratio = new_ratio

    def get_maker_taker_base_diff(self):
        return self.get_entry_value("maker", "base") - self.get_entry_value("taker", "base")

    def get_average_price(self, market):
        if self.get_entry_value(market, "base") != s_decimal_zero:
            return self.get_entry_value(market, "quote") / self.get_entry_value(market, "base")
        else:
            return s_decimal_zero

    def update_entry_ratio(self):
        if self.get_average_price("taker") != s_decimal_zero:
            self._average_ratio = self.get_average_price("maker") / self.get_average_price("taker")
        self._update_ratio = False

    def update_entry(self, market, quote, base):
        if market == "maker":
            self._maker_entry = (Decimal(quote), Decimal(base))
        elif market == "taker":
            self._taker_entry = (Decimal(quote), Decimal(base))

    def start_balancing(self):
        self._balancing_active = True

    def start_ratio_update(self):
        self._update_ratio = True
    
    def trade_in_progress_active(self, is_active):
        self._trade_in_progress = is_active

    def balancing_done(self):
        self._balancing_active = False
        self._maker_sold_in_progress = False

    def bo_ratio_set(self):
        self._set_bo_ratio = False

    def set_order_levels(self):
        self._set_bo_ratio = True

    def set_maker_volume(self, value):
        self._maker_volume = Decimal(str(value))