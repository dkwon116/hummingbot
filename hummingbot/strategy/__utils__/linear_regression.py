import numpy as np
from abc import ABC, abstractmethod
import statsmodels.api as sm
import pandas as pd
from decimal import Decimal


class LinearRegression(ABC):
    def __init__(self, sampling_length: int = 30, fast_sampling_length: int = 10, disparity_length: int = 30):
        self._sampling_length = sampling_length
        self._fast_sampling_length = fast_sampling_length
        self._disparity_length = disparity_length
        self._df = pd.DataFrame()
        self._beta_df = pd.DataFrame()
        self._beta = 0
        self._fast_beta = 0
        self._disparity_var = 0
        self._mean = 0
        self._std = 0

    def add_sample(self, sample):
        maker = sample["maker"]
        taker = sample["taker"]
        last = sample["last"]
        # add data
        df_row = {
            "ts": sample["ts"], 
            "maker_bid": float(maker["bid"]), 
            "maker_ask": float(maker["ask"]), 
            "taker_bid": float(taker["bid"]), 
            "taker_ask": float(taker["ask"]),
            "maker_last": float(last["maker"]), 
            "taker_last": float(last["taker"]),
            }
        self._df = self._df.append(df_row, ignore_index=True)

        # calc mid prices
        self._df["maker_mid"] = (self._df["maker_bid"] + self._df["maker_ask"]) / 2
        self._df["taker_mid"] = (self._df["taker_bid"] + self._df["taker_ask"]) / 2

        # drop first row of sample if over sampling length
        if self._df.shape[0] > self._sampling_length:
            self._df = self._df.iloc[1: , :]
        
        # calculate beta > adjust price > get spread > mean and std
        self._beta = self.calc_beta()
        self._fast_beta = self.calc_fast_beta()

        beta_df_row = {
            "ts": sample["ts"],
            "beta": self._beta,
            "fast_beta": self._fast_beta,
            "last_ratio": float(last["maker"]) / float(last["taker"])
        }

        self._beta_df = self._beta_df.append(beta_df_row, ignore_index=True)
        self._disparity_var = self.calc_disparity_variance()

        adj_taker_ask = self._beta * self._df["taker_ask"]
        adj_taker_bid = self._beta * self._df["taker_bid"]

        mt_ratio = self._df["maker_bid"] / adj_taker_ask - 1
        tm_ratio = adj_taker_bid / self._df["maker_ask"] - 1

        self._mean = (mt_ratio.mean() + tm_ratio.mean()) / 2
        self._std = (mt_ratio.std() + tm_ratio.std()) / 2

    def calc_beta(self):
        return sm.OLS(self._df["maker_mid"].astype(float), self._df["taker_mid"].astype(float)).fit().params["taker_mid"]
    
    def calc_fast_beta(self):
        fast_df = self._df.tail(self._fast_sampling_length)
        return sm.OLS(fast_df["maker_mid"].astype(float), fast_df["taker_mid"].astype(float)).fit().params["taker_mid"]
    
    def calc_disparity_variance(self):
        disparity_df = self._beta_df.tail(self._disparity_length)
        last_diff = disparity_df["last_ratio"] - disparity_df["beta"]
        return np.sqrt(np.sum(np.square(last_diff)) / last_diff.size)

    def samples_filled(self):
        return self._df.shape[0]
    
    @property
    def mean(self):
        return self._mean
    
    @property
    def std(self):
        return self._std
    
    @property
    def beta(self):
        return self._beta
    
    @property
    def fast_beta(self):
        return self._fast_beta
    
    @property
    def disparity_var(self):
        return self._disparity_var
    
    @property
    def premium(self):
        return self._beta - 1
    
    @property
    def fast_premium(self):
        return self._fast_beta - 1