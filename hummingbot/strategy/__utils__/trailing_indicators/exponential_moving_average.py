from .base_trailing_indicator import BaseTrailingIndicator
import pandas as pd


class ExponentialMovingAverageIndicator(BaseTrailingIndicator):
    def __init__(self, sampling_length: int = 30, processing_length: int = 1):
        if processing_length != 1:
            raise Exception("Exponential moving average processing_length should be 1")
        super().__init__(sampling_length, processing_length)
        self._prev_ema = []

    def _indicator_calculation(self) -> float:
        ema = pd.Series(self._sampling_buffer.get_as_numpy_array())\
            .ewm(span=self.sampling_length).mean()
        if len(ema) > 3:
            self._prev_ema.pop(0)
        current_ema = ema[len(ema) - 1]
        self._prev_ema.append(current_ema)
        # self.logger().warning(f"{self._sampling_length} EMA: {self._prev_ema} Up trend {self.is_uptrend}")
        return current_ema

    def _processing_calculation(self) -> float:
        return self._processing_buffer.get_last_value()

    def sampling_length_filled(self) -> float:
        sampling_array = self._sampling_buffer.get_as_numpy_array()
        return sampling_array.size

    def processing_length(self) -> float:
        processing_array = self._processing_buffer.get_as_numpy_array()
        return processing_array.size

    def std_dev(self, length):
        std = pd.Series(self._sampling_buffer.get_as_numpy_array()).rolling(window=length).std()
        return std[len(std) - 1]

    @property
    def is_uptrend(self) -> bool:
        if len(self._prev_ema) > 2:
            return self._processing_buffer.get_last_value() > self._prev_ema[0]
        else:
            False

    @property
    def trend_slope(self) -> bool:
        if len(self._prev_ema) > 2:
            return self._processing_buffer.get_last_value() / self._prev_ema[0] - 1
        else:
            0
