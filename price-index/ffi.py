import mmap
import ctypes
import enum
import pandas as pd
import numpy as np


class Symbols(enum.IntEnum):
    BTC_USD = 0
    ETH_USD = 1
    BNB_USD = 2

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



NUM_SYMBOLS = 3
class TickerBuffer(ctypes.Structure):
    _fields_ = [
        ("data", TickerData * NUM_SYMBOLS * Exchange.NUM_EXCHANGES),
    ]

    def __getitem__(self, exchange: Exchange):
        return self.data[exchange]

    def __repr__(self):
        repr_str = "TickerBuffer(\n"
        for exchange in Exchange:
            if exchange == Exchange.NUM_EXCHANGES:
                continue
            repr_str += f"  {exchange.name}: [\n"
            for symbol_index in range(NUM_SYMBOLS):
                ticker_data = self.data[exchange][symbol_index]
                repr_str += f"    Symbol {symbol_index}: {ticker_data},\n"
            repr_str += "  ],\n"
        repr_str += ")"
        return repr_str


def read_shm(path: str):
    """
    Memory-map 'path' and interpret its contents as a TickerBuffer struct.
    """
    with open(path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ | mmap.ACCESS_WRITE)
        if mm.size() < ctypes.sizeof(TickerBuffer):
            raise ValueError(
                f"File too small: {mm.size()} bytes, need at least {ctypes.sizeof(TickerBuffer)} bytes"
            )
        data = TickerBuffer.from_buffer(mm)
        return data

def to_numpy(ticker_buffer: TickerBuffer):
    return np.frombuffer(
        ticker_buffer.data,
        dtype=TickerData*NUM_SYMBOLS*Exchange.NUM_EXCHANGES
    )


def get_symol_df(ticker_data, symbol_index):
    return pd.DataFrame(ticker_data[0][:, symbol_index], index=[e.name for e in Exchange if e != Exchange.NUM_EXCHANGES], columns=['Bid', 'Ask'],
                        copy=False)


def load_data(shm_path: str):
    return to_numpy(read_shm(shm_path))

