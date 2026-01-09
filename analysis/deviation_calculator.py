"""
Price Deviation Calculator
Calculates and analyzes price deviations between CEX and DEX
"""

import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_init import get_database_client
from analysis.symbol_mapper import SymbolMapper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DeviationStats:
    """Statistics for price deviations"""
    symbol: str
    period_hours: int
    sample_size: int
    mean_deviation: float
    median_deviation: float
    std_deviation: float
    p90_deviation: float
    p95_deviation: float
    p99_deviation: float
    max_deviation: float
    min_deviation: float
    arbitrage_opportunities: int


class PriceDeviationCalculator:
    """Calculates price deviations between CEX and DEX"""
    
    def __init__(self):
        """Initialize deviation calculator"""
        self.db = get_database_client()
        self.mapper = SymbolMapper()
        logger.info("Initialized PriceDeviationCalculator")
    
    def calculate_deviation(self, cex_price: float, dex_price: float) -> float:
        """
        Calculate percentage deviation
        
        Args:
            cex_price: CEX price index
            dex_price: DEX execution price
            
        Returns:
            Deviation percentage (positive = DEX higher, negative = DEX lower)
        """
        if cex_price <= 0:
            return 0.0
        
        deviation = ((dex_price - cex_price) / cex_price) * 100.0
        return deviation
    
    def match_dex_swaps_with_cex_prices(
        self, 
        pool_address: str,
        start_time: datetime,
        end_time: datetime,
        max_time_diff_seconds: int = 2
    ) -> List[Dict]:
        """
        Match DEX swaps with CEX prices at same timestamp
        
        Args:
            pool_address: DEX pool address
            start_time: Start time
            end_time: End time
            max_time_diff_seconds: Maximum allowed time difference
            
        Returns:
            List of matched pairs with deviations
        """
        try:
            cursor = self.db.conn.cursor()
            
            # Get DEX swaps
            cursor.execute("""
                SELECT 
                    ds.time,
                    ds.price as dex_price,
                    ds.amount_in,
                    ds.amount_out,
                    ds.trade_size_usd,
                    ds.tx_hash,
                    t0.symbol as token0_symbol,
                    t1.symbol as token1_symbol
                FROM dex_swaps ds
                LEFT JOIN pools p ON ds.pool_address = p.pool_address
                LEFT JOIN tokens t0 ON p.token0_address = t0.address
                LEFT JOIN tokens t1 ON p.token1_address = t1.address
                WHERE ds.pool_address = %s
                  AND ds.time >= %s
                  AND ds.time <= %s
                ORDER BY ds.time ASC
            """, (pool_address, start_time, end_time))
            
            swaps = cursor.fetchall()
            
            if not swaps:
                logger.warning(f"No swaps found for pool {pool_address}")
                return []
            
            # For each swap, find closest CEX price
            matched_pairs = []
            
            for swap in swaps:
                swap_time = swap[0]
                dex_price = float(swap[1])
                amount_in = float(swap[2])
                amount_out = float(swap[3])
                trade_size_usd = float(swap[4]) if swap[4] else None
                tx_hash = swap[5]
                token0_symbol = swap[6]
                token1_symbol = swap[7]
                
                # Map DEX symbols to CEX format (e.g., WETH/USDT -> ethusdt)
                price_index_symbol = self.mapper.match_pool_to_price_index(
                    token0_symbol, 
                    token1_symbol
                )
                
                if not price_index_symbol:
                    logger.warning(f"Cannot map pool {token0_symbol}/{token1_symbol} to price index")
                    continue
                
                # Construct display symbol (e.g., "ETH/USDT")
                symbol = f"{token0_symbol}/{token1_symbol}"
                
                # Find closest CEX price within time window using normalized symbol
                cursor.execute("""
                    SELECT time, price_index
                    FROM price_index
                    WHERE symbol = %s
                      AND time BETWEEN %s AND %s
                    ORDER BY ABS(EXTRACT(EPOCH FROM (time - %s)))
                    LIMIT 1
                """, (
                    price_index_symbol,  # Use normalized symbol like "ethusdt"
                    swap_time - timedelta(seconds=max_time_diff_seconds),
                    swap_time + timedelta(seconds=max_time_diff_seconds),
                    swap_time
                ))
                
                cex_match = cursor.fetchone()
                
                if cex_match:
                    cex_time = cex_match[0]
                    cex_price = float(cex_match[1])
                    
                    # Calculate time difference
                    time_diff = abs((swap_time - cex_time).total_seconds())
                    
                    if time_diff <= max_time_diff_seconds:
                        # Calculate deviation
                        deviation = self.calculate_deviation(cex_price, dex_price)
                        
                        # Check if arbitrage opportunity
                        arbitrage = abs(deviation) > 0.5  # 0.5% threshold
                        
                        matched_pairs.append({
                            'time': swap_time,
                            'symbol': symbol,
                            'cex_price': cex_price,
                            'dex_price': dex_price,
                            'deviation_pct': deviation,
                            'trade_size_usd': trade_size_usd,
                            'arbitrage_opportunity': arbitrage,
                            'time_diff_seconds': time_diff,
                            'tx_hash': tx_hash
                        })
            
            cursor.close()
            logger.info(f"Matched {len(matched_pairs)} DEX swaps with CEX prices")
            return matched_pairs
            
        except Exception as e:
            logger.error(f"Error matching prices: {e}")
            return []
    
    def calculate_deviation_stats(
        self,
        deviations: List[Dict]
    ) -> Optional[DeviationStats]:
        """
        Calculate statistics for a set of deviations
        
        Args:
            deviations: List of deviation data
            
        Returns:
            DeviationStats object
        """
        if not deviations:
            return None
        
        dev_values = [d['deviation_pct'] for d in deviations]
        dev_array = np.array(dev_values)
        
        arbitrage_count = sum(1 for d in deviations if d.get('arbitrage_opportunity', False))
        
        stats = DeviationStats(
            symbol=deviations[0]['symbol'],
            period_hours=0,  # Will be set by caller
            sample_size=len(deviations),
            mean_deviation=float(np.mean(dev_array)),
            median_deviation=float(np.median(dev_array)),
            std_deviation=float(np.std(dev_array)),
            p90_deviation=float(np.percentile(dev_array, 90)),
            p95_deviation=float(np.percentile(dev_array, 95)),
            p99_deviation=float(np.percentile(dev_array, 99)),
            max_deviation=float(np.max(dev_array)),
            min_deviation=float(np.min(dev_array)),
            arbitrage_opportunities=arbitrage_count
        )
        
        return stats
    
    def store_deviations(self, deviations: List[Dict]) -> int:
        """
        Store price deviations in database
        
        Args:
            deviations: List of deviation data
            
        Returns:
            Number of records stored
        """
        if not deviations:
            return 0
        
        try:
            cursor = self.db.conn.cursor()
            
            for dev in deviations:
                cursor.execute("""
                    INSERT INTO price_deviations 
                        (time, symbol, cex_price, dex_price, deviation_pct, 
                         spread, arbitrage_opportunity)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    dev['time'],
                    dev['symbol'],
                    dev['cex_price'],
                    dev['dex_price'],
                    dev['deviation_pct'],
                    abs(dev['cex_price'] - dev['dex_price']),
                    dev['arbitrage_opportunity']
                ))
            
            self.db.conn.commit()
            cursor.close()
            
            logger.info(f"Stored {len(deviations)} deviation records")
            return len(deviations)
            
        except Exception as e:
            logger.error(f"Error storing deviations: {e}")
            self.db.conn.rollback()
            return 0
    
    def analyze_pool(
        self,
        pool_address: str,
        hours: int = 24
    ) -> Optional[DeviationStats]:
        """
        Complete analysis for a pool
        
        Args:
            pool_address: Pool address
            hours: Hours to analyze
            
        Returns:
            DeviationStats
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        # Match DEX swaps with CEX prices
        matched = self.match_dex_swaps_with_cex_prices(
            pool_address,
            start_time,
            end_time
        )
        
        if not matched:
            logger.warning(f"No matched data for pool {pool_address}")
            return None
        
        # Calculate statistics
        stats = self.calculate_deviation_stats(matched)
        if stats:
            stats.period_hours = hours
        
        # Store deviations
        self.store_deviations(matched)
        
        return stats
    
    def get_recent_deviations(
        self,
        symbol: str,
        hours: int = 24,
        min_deviation: float = 0.1
    ) -> List[Dict]:
        """
        Get recent significant deviations
        
        Args:
            symbol: Trading pair symbol
            hours: Hours to look back
            min_deviation: Minimum absolute deviation to include
            
        Returns:
            List of deviations
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    time,
                    symbol,
                    cex_price,
                    dex_price,
                    deviation_pct,
                    arbitrage_opportunity
                FROM price_deviations
                WHERE symbol = %s
                  AND time >= %s
                  AND ABS(deviation_pct) >= %s
                ORDER BY time DESC
            """, (symbol, cutoff_time, min_deviation))
            
            rows = cursor.fetchall()
            cursor.close()
            
            return [
                {
                    'time': row[0],
                    'symbol': row[1],
                    'cex_price': float(row[2]),
                    'dex_price': float(row[3]),
                    'deviation_pct': float(row[4]),
                    'arbitrage_opportunity': row[5]
                }
                for row in rows
            ]
            
        except Exception as e:
            logger.error(f"Error getting recent deviations: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        self.db.close()


if __name__ == "__main__":
    # Example usage
    calculator = PriceDeviationCalculator()
    
    # Example: Analyze a specific pool
    # pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC/WETH pool
    # stats = calculator.analyze_pool(pool_address, hours=24)
    # 
    # if stats:
    #     logger.info(f"Deviation Stats for {stats.symbol}:")
    #     logger.info(f"  Sample Size: {stats.sample_size}")
    #     logger.info(f"  Mean Deviation: {stats.mean_deviation:.4f}%")
    #     logger.info(f"  Median Deviation: {stats.median_deviation:.4f}%")
    #     logger.info(f"  P95 Deviation: {stats.p95_deviation:.4f}%")
    #     logger.info(f"  Arbitrage Opportunities: {stats.arbitrage_opportunities}")
    
    calculator.close()
    logger.info("Done!")

