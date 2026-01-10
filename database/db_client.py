"""
Database client for crypto exchange data
Handles all interactions with PostgreSQL/TimescaleDB
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CryptoExchangeDB:
    """PostgreSQL/TimescaleDB client for crypto exchange data"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "crypto_exchange",
        user: str = "cmpe492",
        password: str = "password123",
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        self.conn = None
        self.connect()
    
    def connect(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Connected to PostgreSQL")
        except psycopg2.Error as e:
            logger.error(f"❌ Connection failed: {e}")
            raise
    
    @contextmanager
    def get_cursor(self):
        """Get a database cursor"""
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cursor.close()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    # ==================== CEX Ticker Operations ====================
    
    def insert_cex_ticker(self, tickers: List[Dict]) -> int:
        """
        Insert CEX ticker data
        
        Args:
            tickers: List of dicts with keys: time, exchange, symbol, bid, ask, 
                     volume_24h (optional), base_volume (optional), quote_volume (optional)
        
        Returns:
            Number of rows inserted
        """
        query = """
            INSERT INTO cex_tickers (time, exchange, symbol, bid, ask, volume_24h, base_volume, quote_volume)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        try:
            with self.get_cursor() as cur:
                values = [
                    (
                        t['time'],
                        t['exchange'],
                        t['symbol'],
                        float(t['bid']),
                        float(t['ask']),
                        float(t.get('volume_24h', 0)) if t.get('volume_24h') else None,
                        float(t.get('base_volume', 0)) if t.get('base_volume') else None,
                        float(t.get('quote_volume', 0)) if t.get('quote_volume') else None
                    )
                    for t in tickers
                ]
                execute_values(cur, query, values)
                logger.info(f"Inserted {len(values)} CEX ticker records")
                return len(values)
        except Exception as e:
            logger.error(f"Failed to insert CEX tickers: {e}")
            return 0
    
    def get_latest_cex_price(self, symbol: str, exchange: Optional[str] = None):
        """Get latest CEX price for a symbol"""
        query = """
            SELECT time, exchange, symbol, bid, ask, mid_price
            FROM cex_tickers
            WHERE symbol = %s
        """
        params = [symbol]
        
        if exchange:
            query += " AND exchange = %s"
            params.append(exchange)
        
        query += " ORDER BY time DESC LIMIT 1"
        
        with self.get_cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
            if result:
                return {
                    'time': result[0],
                    'exchange': result[1],
                    'symbol': result[2],
                    'bid': float(result[3]),
                    'ask': float(result[4]),
                    'mid_price': float(result[5])
                }
            return None
    
    def get_cex_prices_range(self, symbol: str, hours: int = 24) -> List[Dict]:
        """Get CEX prices for a symbol over time range"""
        query = """
            SELECT time, exchange, symbol, bid, ask, mid_price
            FROM cex_tickers
            WHERE symbol = %s AND time > now() - interval '%s hours'
            ORDER BY time DESC
        """
        with self.get_cursor() as cur:
            cur.execute(query, (symbol, hours))
            results = cur.fetchall()
            return [
                {
                    'time': r[0],
                    'exchange': r[1],
                    'symbol': r[2],
                    'bid': float(r[3]),
                    'ask': float(r[4]),
                    'mid_price': float(r[5])
                }
                for r in results
            ]
    
    # ==================== Price Index Operations ====================
    
    def insert_price_index(self, indices: List[Dict]) -> int:
        """
        Insert price index data
        
        Args:
            indices: List of dicts with keys: time, symbol, price_index, num_exchanges, std_dev
        """
        query = """
            INSERT INTO price_index (time, symbol, price_index, num_exchanges, std_dev)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        try:
            with self.get_cursor() as cur:
                values = [
                    (
                        idx['time'],
                        idx['symbol'],
                        float(idx['price_index']),
                        idx.get('num_exchanges', 0),
                        float(idx.get('std_dev', 0)) if idx.get('std_dev') else None
                    )
                    for idx in indices
                ]
                execute_values(cur, query, values)
                logger.info(f"Inserted {len(values)} price index records")
                return len(values)
        except Exception as e:
            logger.error(f"Failed to insert price index: {e}")
            return 0
    
    # ==================== DEX Swap Operations ====================
    
    def insert_dex_swaps(self, swaps: List[Dict]) -> int:
        """
        Insert DEX swap events
        
        Args:
            swaps: List of dicts with keys: time, chain, dex, pool_address, token_in, 
                   token_out, amount_in, amount_out, price, tx_hash, block_number,
                   trade_size_usd (optional), trade_size_bin (optional), 
                   swap_direction (optional), is_sandwich_victim (optional), 
                   is_arbitrage (optional)
        """
        query = """
            INSERT INTO dex_swaps (time, chain, dex, pool_address, token_in, token_out, 
                                   amount_in, amount_out, price, tx_hash, block_number,
                                   trade_size_usd, trade_size_bin, swap_direction,
                                   is_sandwich_victim, is_arbitrage)
            VALUES %s
        """
        try:
            with self.get_cursor() as cur:
                values = [
                    (
                        s['time'],
                        s['chain'],
                        s['dex'],
                        s['pool_address'],
                        s['token_in'],
                        s['token_out'],
                        float(s['amount_in']),
                        float(s['amount_out']),
                        float(s['price']),
                        s.get('tx_hash'),
                        s.get('block_number'),
                        float(s['trade_size_usd']) if s.get('trade_size_usd') else None,
                        s.get('trade_size_bin'),
                        s.get('swap_direction'),
                        s.get('is_sandwich_victim', False),
                        s.get('is_arbitrage', False)
                    )
                    for s in swaps
                ]
                execute_values(cur, query, values)
                logger.info(f"Inserted {len(values)} DEX swap records")
                return len(values)
        except Exception as e:
            logger.error(f"Failed to insert DEX swaps: {e}")
            return 0
    
    def get_dex_prices(self, token_in: str, token_out: str, hours: int = 24) -> List[Dict]:
        """Get DEX prices for a token pair over time"""
        query = """
            SELECT time, dex, chain, price, amount_in, amount_out
            FROM dex_swaps
            WHERE (token_in = %s AND token_out = %s) 
               OR (token_in = %s AND token_out = %s)
            AND time > now() - interval '%s hours'
            ORDER BY time DESC
        """
        with self.get_cursor() as cur:
            cur.execute(query, (token_in, token_out, token_out, token_in, hours))
            results = cur.fetchall()
            return [
                {
                    'time': r[0],
                    'dex': r[1],
                    'chain': r[2],
                    'price': float(r[3]),
                    'amount_in': float(r[4]),
                    'amount_out': float(r[5])
                }
                for r in results
            ]
    
    # ==================== Token Operations ====================
    
    def upsert_token(self, address: str, symbol: str, decimals: int, 
                     chain: str = "ethereum", logo_url: str = None) -> bool:
        """Insert or update token metadata"""
        query = """
            INSERT INTO tokens (address, symbol, decimals, chain, logo_url)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                decimals = EXCLUDED.decimals,
                logo_url = EXCLUDED.logo_url
        """
        try:
            with self.get_cursor() as cur:
                cur.execute(query, (address, symbol, decimals, chain, logo_url))
                return True
        except Exception as e:
            logger.error(f"Failed to upsert token: {e}")
            return False
    
    def get_token(self, address: str) -> Optional[Dict]:
        """Get token metadata"""
        query = "SELECT address, symbol, decimals, chain FROM tokens WHERE address = %s"
        with self.get_cursor() as cur:
            cur.execute(query, (address,))
            result = cur.fetchone()
            if result:
                return {
                    'address': result[0],
                    'symbol': result[1],
                    'decimals': result[2],
                    'chain': result[3]
                }
            return None
    
    # ==================== Price Deviation Operations ====================
    
    def insert_price_deviations(self, deviations: List[Dict]) -> int:
        """Insert price deviation analysis"""
        query = """
            INSERT INTO price_deviations (time, symbol, cex_price, dex_price, 
                                         deviation_pct, spread, arbitrage_opportunity)
            VALUES %s
        """
        try:
            with self.get_cursor() as cur:
                values = [
                    (
                        d['time'],
                        d['symbol'],
                        float(d['cex_price']),
                        float(d['dex_price']),
                        float(d.get('deviation_pct', 0)),
                        float(d.get('spread', 0)),
                        d.get('arbitrage_opportunity', False)
                    )
                    for d in deviations
                ]
                execute_values(cur, query, values)
                logger.info(f"Inserted {len(values)} price deviation records")
                return len(values)
        except Exception as e:
            logger.error(f"Failed to insert price deviations: {e}")
            return 0
    
    # ==================== CEX Liquidity Operations ====================
    
    def insert_cex_liquidity(self, snapshots: List[Dict]) -> int:
        """
        Insert CEX liquidity snapshots
        
        Args:
            snapshots: List of dicts with keys: time, exchange, symbol, 
                      depth_0_5_pct, depth_1_pct, depth_2_pct, 
                      bid_ask_spread_bps, top_bid, top_ask
        """
        query = """
            INSERT INTO cex_liquidity_snapshot 
                (time, exchange, symbol, depth_0_5_pct, depth_1_pct, depth_2_pct, 
                 bid_ask_spread_bps, top_bid, top_ask)
            VALUES %s
        """
        try:
            with self.get_cursor() as cur:
                values = [
                    (
                        s['time'],
                        s['exchange'],
                        s['symbol'],
                        float(s.get('depth_0_5_pct', 0)) if s.get('depth_0_5_pct') else None,
                        float(s.get('depth_1_pct', 0)) if s.get('depth_1_pct') else None,
                        float(s.get('depth_2_pct', 0)) if s.get('depth_2_pct') else None,
                        s.get('bid_ask_spread_bps'),
                        float(s.get('top_bid', 0)) if s.get('top_bid') else None,
                        float(s.get('top_ask', 0)) if s.get('top_ask') else None
                    )
                    for s in snapshots
                ]
                execute_values(cur, query, values)
                logger.info(f"Inserted {len(values)} CEX liquidity snapshots")
                return len(values)
        except Exception as e:
            logger.error(f"Failed to insert CEX liquidity: {e}")
            return 0
    
    def get_cex_liquidity(self, exchange: str, symbol: str, hours: int = 24) -> List[Dict]:
        """Get CEX liquidity snapshots over time"""
        query = """
            SELECT time, exchange, symbol, depth_0_5_pct, depth_1_pct, depth_2_pct,
                   bid_ask_spread_bps, top_bid, top_ask
            FROM cex_liquidity_snapshot
            WHERE exchange = %s AND symbol = %s
              AND time > now() - interval '%s hours'
            ORDER BY time DESC
        """
        with self.get_cursor() as cur:
            cur.execute(query, (exchange, symbol, hours))
            results = cur.fetchall()
            return [
                {
                    'time': r[0],
                    'exchange': r[1],
                    'symbol': r[2],
                    'depth_0_5_pct': float(r[3]) if r[3] else None,
                    'depth_1_pct': float(r[4]) if r[4] else None,
                    'depth_2_pct': float(r[5]) if r[5] else None,
                    'bid_ask_spread_bps': r[6],
                    'top_bid': float(r[7]) if r[7] else None,
                    'top_ask': float(r[8]) if r[8] else None
                }
                for r in results
            ]
    
    # ==================== DEX Pool State Operations ====================
    
    def insert_pool_states(self, states: List[Dict]) -> int:
        """
        Insert DEX pool state snapshots
        
        Args:
            states: List of dicts with keys: time, pool_address, chain, dex,
                   reserve0, reserve1, sqrt_price_x96, tick, liquidity,
                   tvl_usd, price, block_number, triggered_by_tx
        """
        query = """
            INSERT INTO dex_pool_state 
                (time, pool_address, chain, dex, reserve0, reserve1, 
                 sqrt_price_x96, tick, liquidity, tvl_usd, price, 
                 block_number, triggered_by_tx)
            VALUES %s
        """
        try:
            with self.get_cursor() as cur:
                values = [
                    (
                        s['time'],
                        s['pool_address'],
                        s['chain'],
                        s['dex'],
                        float(s.get('reserve0', 0)) if s.get('reserve0') else None,
                        float(s.get('reserve1', 0)) if s.get('reserve1') else None,
                        int(s['sqrt_price_x96']) if s.get('sqrt_price_x96') else None,
                        s.get('tick'),
                        int(s['liquidity']) if s.get('liquidity') else None,
                        float(s.get('tvl_usd', 0)) if s.get('tvl_usd') else None,
                        float(s.get('price', 0)) if s.get('price') else None,
                        s.get('block_number'),
                        s.get('triggered_by_tx')
                    )
                    for s in states
                ]
                execute_values(cur, query, values)
                logger.info(f"Inserted {len(values)} pool state records")
                return len(values)
        except Exception as e:
            logger.error(f"Failed to insert pool states: {e}")
            return 0
    
    def get_pool_states(self, pool_address: str, hours: int = 24) -> List[Dict]:
        """Get pool state history over time"""
        query = """
            SELECT time, pool_address, chain, dex, reserve0, reserve1,
                   sqrt_price_x96, tick, liquidity, tvl_usd, price,
                   block_number, triggered_by_tx
            FROM dex_pool_state
            WHERE pool_address = %s
              AND time > now() - interval '%s hours'
            ORDER BY time DESC
        """
        with self.get_cursor() as cur:
            cur.execute(query, (pool_address, hours))
            results = cur.fetchall()
            return [
                {
                    'time': r[0],
                    'pool_address': r[1],
                    'chain': r[2],
                    'dex': r[3],
                    'reserve0': float(r[4]) if r[4] else None,
                    'reserve1': float(r[5]) if r[5] else None,
                    'sqrt_price_x96': int(r[6]) if r[6] else None,
                    'tick': r[7],
                    'liquidity': int(r[8]) if r[8] else None,
                    'tvl_usd': float(r[9]) if r[9] else None,
                    'price': float(r[10]) if r[10] else None,
                    'block_number': r[11],
                    'triggered_by_tx': r[12]
                }
                for r in results
            ]
    
    # ==================== Data Ingestion Logging ====================
    
    def log_ingestion(self, service_name: str, event_type: str, status: str, 
                     message: str = "", records_processed: int = 0):
        """Log data ingestion event"""
        query = """
            INSERT INTO data_ingestion_logs (service_name, event_type, status, message, records_processed)
            VALUES (%s, %s, %s, %s, %s)
        """
        try:
            with self.get_cursor() as cur:
                cur.execute(query, (service_name, event_type, status, message, records_processed))
        except Exception as e:
            logger.error(f"Failed to log ingestion: {e}")
    
    # ==================== Helper Utilities ====================
    
    @staticmethod
    def classify_trade_size(trade_size_usd: float) -> str:
        """
        Classify trade size into bins
        
        Args:
            trade_size_usd: Trade size in USD
            
        Returns:
            Trade size bin label
        """
        if trade_size_usd < 1000:
            return '<1k'
        elif trade_size_usd < 10000:
            return '1k-10k'
        elif trade_size_usd < 100000:
            return '10k-100k'
        elif trade_size_usd < 1000000:
            return '100k-1M'
        else:
            return '>1M'
    
    @staticmethod
    def calculate_bid_ask_spread_bps(bid: float, ask: float) -> int:
        """
        Calculate bid-ask spread in basis points
        
        Args:
            bid: Bid price
            ask: Ask price
            
        Returns:
            Spread in basis points (1 bp = 0.01%)
        """
        if bid <= 0 or ask <= 0:
            return 0
        mid = (bid + ask) / 2
        spread_pct = ((ask - bid) / mid) * 100
        return int(spread_pct * 100)  # Convert to basis points
    
    # ==================== Query Utilities ====================
    
    def health_check(self) -> bool:
        """Check database health"""
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


if __name__ == "__main__":
    # Test connection
    db = CryptoExchangeDB()
    
    if db.health_check():
        print("Database is healthy")
    else:
        print("❌ Database health check failed")
    
    db.close()
