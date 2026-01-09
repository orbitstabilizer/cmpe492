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
            logger.info("✅ Connected to PostgreSQL")
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
            tickers: List of dicts with keys: time, exchange, symbol, bid, ask
        
        Returns:
            Number of rows inserted
        """
        query = """
            INSERT INTO cex_tickers (time, exchange, symbol, bid, ask)
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
                        float(t['ask'])
                    )
                    for t in tickers
                ]
                execute_values(cur, query, values)
                logger.info(f"✅ Inserted {len(values)} CEX ticker records")
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
                logger.info(f"✅ Inserted {len(values)} price index records")
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
                   token_out, amount_in, amount_out, price, tx_hash, block_number
        """
        query = """
            INSERT INTO dex_swaps (time, chain, dex, pool_address, token_in, token_out, 
                                   amount_in, amount_out, price, tx_hash, block_number)
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
                        s.get('block_number')
                    )
                    for s in swaps
                ]
                execute_values(cur, query, values)
                logger.info(f"✅ Inserted {len(values)} DEX swap records")
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
    
    def upsert_token(self, address: str, symbol: str, name: str, decimals: int, 
                     chain: str = "ethereum", logo_url: str = None) -> bool:
        """Insert or update token metadata"""
        query = """
            INSERT INTO tokens (address, symbol, name, decimals, chain, logo_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                decimals = EXCLUDED.decimals,
                logo_url = EXCLUDED.logo_url
        """
        try:
            with self.get_cursor() as cur:
                cur.execute(query, (address, symbol, name, decimals, chain, logo_url))
                return True
        except Exception as e:
            logger.error(f"Failed to upsert token: {e}")
            return False
    
    def get_token(self, address: str) -> Optional[Dict]:
        """Get token metadata"""
        query = "SELECT address, symbol, name, decimals, chain FROM tokens WHERE address = %s"
        with self.get_cursor() as cur:
            cur.execute(query, (address,))
            result = cur.fetchone()
            if result:
                return {
                    'address': result[0],
                    'symbol': result[1],
                    'name': result[2],
                    'decimals': result[3],
                    'chain': result[4]
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
                logger.info(f"✅ Inserted {len(values)} price deviation records")
                return len(values)
        except Exception as e:
            logger.error(f"Failed to insert price deviations: {e}")
            return 0
    
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
        print("✅ Database is healthy")
    else:
        print("❌ Database health check failed")
    
    db.close()
