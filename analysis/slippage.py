"""
Slippage Analysis Module
Analyzes execution slippage in DEX trades
"""

import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_init import get_database_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class SlippageResult:
    """Result of slippage analysis"""
    pool_address: str
    time_period: str
    total_swaps: int
    avg_slippage_bps: float
    median_slippage_bps: float
    p90_slippage_bps: float
    p95_slippage_bps: float
    p99_slippage_bps: float
    worst_slippage_bps: float
    slippage_by_size: Dict[str, float]


class SlippageAnalyzer:
    """Analyzes slippage in DEX trades"""
    
    def __init__(self):
        """Initialize slippage analyzer"""
        self.db = get_database_client()
        logger.info("Initialized SlippageAnalyzer")
    
    def calculate_uniswap_v2_theoretical_slippage(
        self,
        reserve_in: float,
        reserve_out: float,
        amount_in: float,
        fee_bps: int = 30
    ) -> float:
        """
        Calculate theoretical slippage for Uniswap V2 constant product AMM
        
        Args:
            reserve_in: Input token reserve
            reserve_out: Output token reserve
            amount_in: Trade size (input tokens)
            fee_bps: Fee in basis points (default 30 = 0.3%)
            
        Returns:
            Slippage in basis points
        """
        if reserve_in <= 0 or reserve_out <= 0 or amount_in <= 0:
            return 0.0
        
        # Current price (before trade)
        price_before = reserve_out / reserve_in
        
        # Amount in after fee
        fee_multiplier = (10000 - fee_bps) / 10000
        amount_in_after_fee = amount_in * fee_multiplier
        
        # Amount out from constant product formula
        # amount_out = (amount_in * reserve_out) / (reserve_in + amount_in)
        amount_out = (amount_in_after_fee * reserve_out) / (reserve_in + amount_in_after_fee)
        
        # Execution price
        execution_price = amount_out / amount_in
        
        # Slippage = (price_before - execution_price) / price_before
        slippage = ((price_before - execution_price) / price_before) * 10000  # in bps
        
        return slippage
    
    def calculate_empirical_slippage(
        self,
        expected_price: float,
        execution_price: float
    ) -> float:
        """
        Calculate empirical slippage from actual trade
        
        Args:
            expected_price: Expected price (e.g., from CEX or pre-trade quote)
            execution_price: Actual execution price
            
        Returns:
            Slippage in basis points (positive = worse than expected)
        """
        if expected_price <= 0:
            return 0.0
        
        slippage_bps = ((expected_price - execution_price) / expected_price) * 10000
        return slippage_bps
    
    def analyze_pool_slippage(
        self,
        pool_address: str,
        start_time: datetime,
        end_time: datetime
    ) -> Optional[SlippageResult]:
        """
        Analyze slippage for a pool
        
        Args:
            pool_address: Pool address
            start_time: Start time
            end_time: End time
            
        Returns:
            SlippageResult object
        """
        try:
            cursor = self.db.conn.cursor()
            
            # Get all swaps with pool state
            cursor.execute("""
                WITH swap_with_state AS (
                    SELECT 
                        ds.time,
                        ds.amount_in,
                        ds.amount_out,
                        ds.price as execution_price,
                        CASE 
                            WHEN tin.symbol = 'USDT' THEN ds.amount_in 
                            ELSE ds.amount_out 
                        END as trade_size_usd,
                        ps.reserve0,
                        ps.reserve1,
                        ps.price as pool_price
                    FROM dex_swaps ds
                    LEFT JOIN tokens tin ON ds.token_in = tin.address
                    LEFT JOIN LATERAL (
                        SELECT reserve0, reserve1, price
                        FROM dex_pool_state
                        WHERE pool_address = ds.pool_address
                          AND time <= ds.time
                        ORDER BY time DESC
                        LIMIT 1
                    ) ps ON true
                    WHERE ds.pool_address = %s
                      AND ds.time >= %s
                      AND ds.time <= %s
                )
                SELECT * FROM swap_with_state
                ORDER BY time ASC
            """, (pool_address, start_time, end_time))
            
            swaps = cursor.fetchall()
            cursor.close()
            
            if not swaps:
                logger.warning(f"No swaps found for pool {pool_address}")
                return None
            
            slippages = []
            slippage_by_size = {'small': [], 'medium': [], 'large': [], 'whale': []}
            
            for swap in swaps:
                time = swap[0]
                amount_in = float(swap[1])
                execution_price = float(swap[3])
                trade_size_usd = float(swap[4]) if swap[4] else 0.0
                
                # Determine bin dynamically
                trade_size_bin = 'small'
                if trade_size_usd >= 100000:
                    trade_size_bin = 'whale'
                elif trade_size_usd >= 10000:
                    trade_size_bin = 'large'
                elif trade_size_usd >= 1000:
                    trade_size_bin = 'medium'
                    
                reserve0 = float(swap[5]) if swap[5] else None
                pool_price = float(swap[7]) if swap[7] else None
                
                # Calculate slippage if we have pool state
                if pool_price and pool_price > 0:
                    slippage_bps = self.calculate_empirical_slippage(
                        pool_price,
                        execution_price
                    )
                    slippages.append(abs(slippage_bps))
                    
                    # Group by size
                    if trade_size_bin in slippage_by_size:
                        slippage_by_size[trade_size_bin].append(abs(slippage_bps))
            
            if not slippages:
                logger.warning(f"Could not calculate slippage for pool {pool_address}")
                return None
            
            # Calculate statistics
            slippage_array = np.array(slippages)
            
            avg_by_size = {}
            for size, values in slippage_by_size.items():
                if values:
                    avg_by_size[size] = float(np.mean(values))
                else:
                    avg_by_size[size] = 0.0
            
            result = SlippageResult(
                pool_address=pool_address,
                time_period=f"{start_time} to {end_time}",
                total_swaps=len(slippages),
                avg_slippage_bps=float(np.mean(slippage_array)),
                median_slippage_bps=float(np.median(slippage_array)),
                p90_slippage_bps=float(np.percentile(slippage_array, 90)),
                p95_slippage_bps=float(np.percentile(slippage_array, 95)),
                p99_slippage_bps=float(np.percentile(slippage_array, 99)),
                worst_slippage_bps=float(np.max(slippage_array)),
                slippage_by_size=avg_by_size
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing slippage: {e}")
            return None
    
    def store_slippage_analysis(
        self,
        result: SlippageResult
    ) -> bool:
        """
        Store slippage analysis results
        
        Args:
            result: SlippageResult object
            
        Returns:
            True if successful
        """
        try:
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                INSERT INTO slippage_analysis
                    (time, pool_address, time_period, avg_slippage_bps,
                     max_slippage_bps, slippage_distribution, trade_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                datetime.utcnow(),
                result.pool_address,
                result.time_period,
                result.avg_slippage_bps,
                result.worst_slippage_bps,
                str(result.slippage_by_size),
                result.total_swaps
            ))
            
            self.db.conn.commit()
            cursor.close()
            
            logger.info(f"Stored slippage analysis for {result.pool_address}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing slippage analysis: {e}")
            self.db.conn.rollback()
            return False
    
    def compare_theoretical_vs_actual(
        self,
        pool_address: str,
        hours: int = 24
    ) -> Dict:
        """
        Compare theoretical AMM slippage vs actual
        
        Args:
            pool_address: Pool address
            hours: Hours to analyze
            
        Returns:
            Comparison statistics
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        try:
            cursor = self.db.conn.cursor()
            
            # Get swaps with pool state
            cursor.execute("""
                SELECT 
                    ds.amount_in,
                    ds.amount_out,
                    ds.price as execution_price,
                    ps.reserve0,
                    ps.reserve1,
                    ps.price as pool_price_before
                FROM dex_swaps ds
                LEFT JOIN LATERAL (
                    SELECT reserve0, reserve1, price
                    FROM dex_pool_state
                    WHERE pool_address = ds.pool_address
                      AND time < ds.time
                    ORDER BY time DESC
                    LIMIT 1
                ) ps ON true
                WHERE ds.pool_address = %s
                  AND ds.time >= %s
                  AND ds.time <= %s
                  AND ps.reserve0 IS NOT NULL
                LIMIT 1000
            """, (pool_address, start_time, end_time))
            
            swaps = cursor.fetchall()
            cursor.close()
            
            if not swaps:
                return {'error': 'No data available'}
            
            theoretical_slippages = []
            actual_slippages = []
            
            for swap in swaps:
                amount_in = float(swap[0])
                amount_out = float(swap[1])
                execution_price = float(swap[2])
                reserve0 = float(swap[3])
                reserve1 = float(swap[4])
                pool_price_before = float(swap[5])
                
                # Theoretical slippage
                theoretical = self.calculate_uniswap_v2_theoretical_slippage(
                    reserve0, reserve1, amount_in
                )
                theoretical_slippages.append(theoretical)
                
                # Actual slippage
                actual = self.calculate_empirical_slippage(
                    pool_price_before,
                    execution_price
                )
                actual_slippages.append(abs(actual))
            
            theo_array = np.array(theoretical_slippages)
            actual_array = np.array(actual_slippages)
            
            return {
                'samples': len(swaps),
                'avg_theoretical_bps': float(np.mean(theo_array)),
                'avg_actual_bps': float(np.mean(actual_array)),
                'median_theoretical_bps': float(np.median(theo_array)),
                'median_actual_bps': float(np.median(actual_array)),
                'correlation': float(np.corrcoef(theo_array, actual_array)[0, 1]) if len(theo_array) > 1 else 0.0
            }
            
        except Exception as e:
            logger.error(f"Error comparing slippage: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Close database connection"""
        self.db.close()


if __name__ == "__main__":
    # Example usage
    analyzer = SlippageAnalyzer()
    
    # Example: Analyze a pool
    # pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
    # end_time = datetime.utcnow()
    # start_time = end_time - timedelta(hours=24)
    # 
    # result = analyzer.analyze_pool_slippage(pool_address, start_time, end_time)
    # if result:
    #     logger.info(f"Slippage Analysis for {result.pool_address}:")
    #     logger.info(f"  Total Swaps: {result.total_swaps}")
    #     logger.info(f"  Avg Slippage: {result.avg_slippage_bps:.2f} bps")
    #     logger.info(f"  Median Slippage: {result.median_slippage_bps:.2f} bps")
    #     logger.info(f"  P95 Slippage: {result.p95_slippage_bps:.2f} bps")
    #     logger.info(f"  Worst Slippage: {result.worst_slippage_bps:.2f} bps")
    
    analyzer.close()
    logger.info("Done!")

