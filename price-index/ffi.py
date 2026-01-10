import mmap
import ctypes
import enum
import os
import pandas as pd
import numpy as np
import json


class Symbols(enum.IntEnum):
    pass

class Exchange(enum.IntEnum):
    pass


config_path = os.getenv("EXCHANGE_INFO_PATH", "exchange_info.json")
with open(config_path, "r") as f:
    exchange_info = json.load(f)
    pairs = exchange_info["symbols"][2]
    sb = "class Symbols(enum.IntEnum):\n"
    for i, pair in enumerate(pairs):
        sb += f"    {pair.replace('-', '_')} = {i}\n"
    exec(sb)

    sb = "class Exchange(enum.IntEnum):\n"
    exchanges = exchange_info["exchanges"]
    for i, exchange in enumerate(exchanges):
        sb += f"    {exchange} = {i}\n"
    exec(sb)


class TickerData(ctypes.Structure):
    _fields_ = [
        ("Bid", ctypes.c_double),
        ("Ask", ctypes.c_double),
        ("BidQty", ctypes.c_double),
        ("AskQty", ctypes.c_double),
    ]

    def __repr__(self):
        return f"TickerData(Bid={self.Bid}, Ask={self.Ask}, BidQty={self.BidQty}, AskQty={self.AskQty})"


class PriceIndex(ctypes.Structure):
    _fields_ = [
        ("Val", ctypes.c_double),
        ("Cnt", ctypes.c_int64),
        ("BidVWAP", ctypes.c_double),
        ("BidQtyTotal", ctypes.c_double),
        ("AskVWAP", ctypes.c_double),
        ("AskQtyTotal", ctypes.c_double),
    ]

    def __repr__(self):
        return f"PriceIndex(Val={self.Val}, Cnt={self.Cnt})"



NUM_SYMBOLS = 128 # preallocated number of symbols
class ShmLayout(ctypes.Structure):
    _fields_ = [
        ("tickers", TickerData * NUM_SYMBOLS * len(Exchange)),
        ("price_indices", PriceIndex * len(Symbols)),
    ]

    def __getitem__(self, exchange: Exchange):
        return self.tickers[exchange]

    def __repr__(self):
        repr_str = "TickerBuffer(\n"
        for exchange in Exchange:
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
            "tickers": np.frombuffer(data.tickers, dtype=TickerData * NUM_SYMBOLS * len(Exchange))[:len(symbols)],
            "price_indices": np.frombuffer(data.price_indices, dtype=PriceIndex * len(Symbols))[0],

        }

exchanges = [e.name for e in Exchange]
symbols = [s.name for s in Symbols]

symbol_to_value = {
    sym.name: sym.value
    for sym in
    Symbols
}

def get_ticker_dfs(ticker_data):
    return {
        sym.name:
        pd.DataFrame(ticker_data[0][:, sym.value], index=exchanges, columns=['Bid', 'Ask', 'BidQty', 'AskQty'], # type: ignore
                        copy=False)
        for sym in Symbols
    }


def get_price_indices_df(price_indices_data):
    return pd.DataFrame(
        price_indices_data, index=symbols, # type: ignore
                        copy=False)
