from .base_trailing_indicator import BaseTrailingIndicator
import numpy as np


class BollingerBandIndicator(BaseTrailingIndicator):
    def __init__(self, sampling_length: int = 30, processing_length: int = 30):
        super().__init__(sampling_length, processing_length)
    

    def _indicator_calculation(self) -> float:
        return np.mean(self._sampling_buffer.get_as_numpy_array())

    def _processing_calculation(self) -> float:
        return self._processing_buffer.get_last_value()
    
    def get_stdev(self) -> float:
        sampling_array = self._sampling_buffer.get_as_numpy_array()
        if sampling_array.size > 0:
            return np.std(sampling_array)
        else: 
            return 0

    def sampling_length_filled(self) -> float:
        sampling_array = self._sampling_buffer.get_as_numpy_array()
        return sampling_array.size

    def processing_length(self) -> float:
        processing_array = self._processing_buffer.get_as_numpy_array()
        return processing_array.size

    def upper(self, factor) -> float:
        return self._processing_calculation() + (self.get_stdev() * factor)
    
    def lower(self, factor) -> float:
        return self._processing_calculation() - (self.get_stdev() * factor)
    
    def normalize(self, value):
        return value / self._processing_calculation() - 1
    
    def normalized_base(self) -> float:
        return self.normalize(self._processing_calculation())

    def normalized_upper(self, factor) -> float:
        return self.normalize(self.upper(factor))

    def normalized_lower(self, factor) -> float:
        return self.normalize(self.lower(factor))
    
    def normalized_stdev(self) -> float:
        return self.normalize(self.get_stdev())