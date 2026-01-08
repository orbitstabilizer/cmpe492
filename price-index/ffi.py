import mmap
import ctypes
import enum
import pandas as pd
import numpy as np


class Symbols(enum.IntEnum):
    BTC_USDT = 0
    ETH_USDT = 1
    BNB_USDT = 2

class TickerData(ctypes.Structure):
    _fields_ = [
        ("Bid", ctypes.c_double),
        ("Ask", ctypes.c_double),
    ]

    def __repr__(self):
        return f"TickerData(Bid={self.Bid}, Ask={self.Ask})"

class Exchange(enum.IntEnum):
    Binance = 0
    Bybit = enum.auto()
    Coinbase = enum.auto()
    Gateio = enum.auto()
    HTX = enum.auto()
    Kucoin = enum.auto()
    Mexc = enum.auto()
    OKX = enum.auto()
    NUM_EXCHANGES = enum.auto()



NUM_SYMBOLS = 128 # preallocated number of symbols
class ShmLayout(ctypes.Structure):
    _fields_ = [
        ("tickers", TickerData * NUM_SYMBOLS * Exchange.NUM_EXCHANGES),
        ("price_indices", ctypes.c_double * len(Symbols)),
    ]

    def __getitem__(self, exchange: Exchange):
        return self.tickers[exchange]

    def __repr__(self):
        repr_str = "TickerBuffer(\n"
        for exchange in Exchange:
            if exchange == Exchange.NUM_EXCHANGES:
                continue
            repr_str += f"  {exchange.name}: [\n"
            for symbol_index in range(NUM_SYMBOLS):
                ticker_data = self.tickers[exchange][symbol_index]
                repr_str += f"    Symbol {symbol_index}: {ticker_data},\n"
            repr_str += "  ],\n"
        repr_str += ")"
        return repr_str


def read_shm(path: str):
    """
    Memory-map 'path' and interpret its contents as a ShmLayout structure.
    Returns a dictionary with 'tickers' and 'price_indices' numpy arrays.
    """
    with open(path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ | mmap.ACCESS_WRITE)
        if mm.size() < ctypes.sizeof(ShmLayout):
            raise ValueError(
                f"File too small: {mm.size()} bytes, need at least {ctypes.sizeof(ShmLayout)} bytes"
            )
        data = ShmLayout.from_buffer(mm)
        return {
            "tickers": np.frombuffer(data.tickers, dtype=TickerData * NUM_SYMBOLS * Exchange.NUM_EXCHANGES)[:len(symbols)],
            "price_indices": np.frombuffer(data.price_indices, dtype=ctypes.c_double * len(Symbols)).reshape(-1, 1),

        }

exchanges = [e.name for e in Exchange if e != Exchange.NUM_EXCHANGES]
symbols = [s.name for s in Symbols]

symbol_to_value = {
    sym.name: sym.value
    for sym in
    Symbols
}

def get_ticker_dfs(ticker_data):
    return {
        sym.name:
        pd.DataFrame(ticker_data[0][:, sym.value], index=exchanges, columns=['Bid', 'Ask'], # type: ignore
                        copy=False)
        for sym in Symbols
    }


def get_price_indices_df(price_indices_data):
    return pd.DataFrame(
        price_indices_data, index=symbols, columns=['Price Index'], # type: ignore
                        copy=False)
