"""
Lead-Lag Correlation Analysis Module
Analyzes price correlations and lead-lag relationships between CEX and DEX
"""

import logging
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import json
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_init import get_database_client
from analysis.symbol_mapper import SymbolMapper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class CorrelationResult:
    """Result of correlation analysis"""
    symbol: str
    period: str
    cex_dex_correlation: float
    dex_leading: bool
    lead_lag_periods: int
    lead_lag_seconds: float
    cex_volatility: float
    dex_volatility: float
    price_deviation_mean: float
    price_deviation_std: float


class LeadLagAnalyzer:
    """Analyzes lead-lag relationships between CEX and DEX prices"""
    
    def __init__(self, window_size: int = 300):
        """
        Initialize analyzer
        
        Args:
            window_size: Number of observations for moving correlation (seconds)
        """
        self.db = get_database_client()
        self.mapper = SymbolMapper()
        self.window_size = window_size
        logger.info(f"Initialized LeadLagAnalyzer (window={window_size}s)")
    
    def get_cex_prices(self, symbol: str, start_time: datetime, 
                       end_time: datetime, interval: int = 60) -> Dict:
        """
        Get CEX prices within time range
        
        Args:
            symbol: Trading pair in display format (e.g., "BTC/USDT", "ETH/USDT")
            start_time: Start datetime
            end_time: End datetime
            interval: Aggregation interval in seconds
        
        Returns:
            Dictionary with timestamps and prices
        """
        try:
            # Convert display symbol to price_index format
            # "BTC/USDT" -> "btcusdt", "ETH/USDT" -> "ethusdt"
            if '/' in symbol:
                base, quote = symbol.split('/')
                price_index_symbol = self.mapper.create_price_index_symbol(base, quote)
            else:
                # Already in price_index format
                price_index_symbol = symbol.lower()
            
            # Get price index data
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT time, price_index, std_dev
                FROM price_index
                WHERE symbol = %s 
                  AND time >= %s 
                  AND time <= %s
                ORDER BY time ASC
            """, (price_index_symbol, start_time, end_time))
            
            rows = cursor.fetchall()
            cursor.close()
            
            if not rows:
                logger.warning(f"No CEX prices found for {symbol}")
                return {}
            
            # Aggregate by interval
            prices = {
                'times': [row[0] for row in rows],
                'prices': [row[1] for row in rows],
                'std_devs': [row[2] for row in rows]
            }
            
            return prices
        
        except Exception as e:
            logger.error(f"Error fetching CEX prices: {e}")
            return {}
    
    def get_dex_prices(self, token_pair: Tuple[str, str], start_time: datetime,
                       end_time: datetime, chain: str = "ethereum") -> Dict:
        """
        Get DEX prices from swap events
        
        Args:
            token_pair: Tuple of (token_in_address, token_out_address)
            start_time: Start datetime
            end_time: End datetime
            chain: Blockchain name
        
        Returns:
            Dictionary with timestamps and prices
        """
        try:
            token_in, token_out = token_pair
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT time, price, amount_in, amount_out
                FROM dex_swaps
                WHERE (token_in = %s AND token_out = %s)
                  AND time >= %s 
                  AND time <= %s
                  AND chain = %s
                ORDER BY time ASC
            """, (token_in, token_out, start_time, end_time, chain))
            
            rows = cursor.fetchall()
            cursor.close()
            
            if not rows:
                logger.warning(f"No DEX swaps found for {token_in}→{token_out}")
                return {}
            
            # Extract prices and check if they need to be inverted
            raw_prices = [float(row[1]) for row in rows]
            
            # Detect if prices are inverted (should be close to CEX price, not tiny values)
            # If median price < 1, it's likely inverted (e.g., USDT/BNB instead of BNB/USDT)
            median_price = np.median(raw_prices) if raw_prices else 0
            
            # Invert prices if they seem to be the wrong way around
            if median_price < 10:  # Threshold: if < $10, likely inverted
                corrected_prices = [1.0 / p if p > 0 else 0 for p in raw_prices]
                logger.info(f"Inverted DEX prices (median was {median_price:.6f}, now {np.median([p for p in corrected_prices if p > 0]):.2f})")
            else:
                corrected_prices = raw_prices
            
            prices = {
                'times': [row[0] for row in rows],
                'prices': corrected_prices,
                'amounts_in': [float(row[2]) for row in rows],
                'amounts_out': [float(row[3]) for row in rows]
            }
            
            return prices
        
        except Exception as e:
            logger.error(f"Error fetching DEX prices: {e}")
            return {}
    
    def calculate_correlation(self, cex_prices: List[float], 
                             dex_prices: List[float]) -> float:
        """
        Calculate Pearson correlation
        
        Args:
            cex_prices: CEX price series
            dex_prices: DEX price series
        
        Returns:
            Correlation coefficient (-1 to 1)
        """
        if len(cex_prices) < 2 or len(dex_prices) < 2:
            return 0.0
        
        try:
            # Normalize to same length
            min_len = min(len(cex_prices), len(dex_prices))
            cex = np.array(cex_prices[:min_len], dtype=float)
            dex = np.array(dex_prices[:min_len], dtype=float)
            
            # Handle NaN/inf
            mask = np.isfinite(cex) & np.isfinite(dex)
            if np.sum(mask) < 2:
                return 0.0
            
            correlation = np.corrcoef(cex[mask], dex[mask])[0, 1]
            return float(correlation) if not np.isnan(correlation) else 0.0
        
        except Exception as e:
            logger.error(f"Error calculating correlation: {e}")
            return 0.0
    
    def granger_causality_test(
        self,
        cex_prices: List[float],
        dex_prices: List[float],
        max_lag: int = 10
    ) -> Dict:
        """
        Perform Granger causality test
        
        Args:
            cex_prices: CEX price series
            dex_prices: DEX price series
            max_lag: Maximum lag to test
            
        Returns:
            Dictionary with test results
        """
        try:
            from statsmodels.tsa.stattools import grangercausalitytests
            
            # Prepare data
            min_len = min(len(cex_prices), len(dex_prices))
            if min_len < max_lag + 10:
                return {'error': 'Insufficient data'}
            
            cex = np.array(cex_prices[:min_len])
            dex = np.array(dex_prices[:min_len])
            
            # Remove NaN/inf
            mask = np.isfinite(cex) & np.isfinite(dex)
            cex = cex[mask]
            dex = dex[mask]
            
            if len(cex) < max_lag + 10:
                return {'error': 'Too many invalid values'}
            
            # Create dataframe for test (DEX as dependent, CEX as independent)
            data = np.column_stack([dex, cex])
            
            # Run test
            results = grangercausalitytests(data, max_lag, verbose=False)
            
            # Extract p-values for each lag
            p_values = {}
            for lag in range(1, max_lag + 1):
                # Use F-test p-value
                p_val = results[lag][0]['ssr_ftest'][1]
                p_values[lag] = p_val
            
            # Find minimum p-value (strongest causality)
            min_p_lag = min(p_values, key=p_values.get)
            min_p_value = p_values[min_p_lag]
            
            return {
                'cex_granger_causes_dex': min_p_value < 0.05,
                'min_p_value': min_p_value,
                'optimal_lag': min_p_lag,
                'all_p_values': p_values
            }
            
        except ImportError:
            logger.error("statsmodels not installed. Install with: pip install statsmodels")
            return {'error': 'statsmodels not installed'}
        except Exception as e:
            logger.error(f"Granger causality test failed: {e}")
            return {'error': str(e)}
    
    def calculate_lead_lag(self, cex_prices: List[float], 
                          dex_prices: List[float],
                          max_lag: int = 50) -> Tuple[int, float]:
        """
        Calculate lead-lag relationship using cross-correlation
        
        Args:
            cex_prices: CEX price series
            dex_prices: DEX price series
            max_lag: Maximum lag to test (in periods)
        
        Returns:
            (lag_periods, peak_correlation)
            Positive lag = DEX leads CEX
            Negative lag = CEX leads DEX
        """
        if len(cex_prices) < max_lag or len(dex_prices) < max_lag:
            return 0, 0.0
        
        try:
            cex = np.array(cex_prices, dtype=float)
            dex = np.array(dex_prices, dtype=float)
            
            # Normalize
            cex = (cex - np.nanmean(cex)) / (np.nanstd(cex) + 1e-10)
            dex = (dex - np.nanmean(dex)) / (np.nanstd(dex) + 1e-10)
            
            # Calculate cross-correlation
            max_corr = -2.0
            best_lag = 0
            
            for lag in range(-max_lag, max_lag + 1):
                if lag < 0:
                    # DEX leads CEX
                    corr = np.nanmean(dex[:lag] * cex[-lag:])
                elif lag > 0:
                    # CEX leads DEX
                    corr = np.nanmean(cex[:lag] * dex[lag:])
                else:
                    # No lag
                    corr = np.nanmean(cex * dex)
                
                if corr > max_corr:
                    max_corr = corr
                    best_lag = lag
            
            return best_lag, float(max_corr)
        
        except Exception as e:
            logger.error(f"Error calculating lead-lag: {e}")
            return 0, 0.0
    
    def analyze_symbol(self, symbol: str, hours: int = 24) -> Optional[CorrelationResult]:
        """
        Full analysis for a trading pair
        
        Args:
            symbol: Trading pair (e.g., "BTC/USDT", "ETH/USDT")
            hours: Historical period
        
        Returns:
            CorrelationResult or None
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        # Get CEX prices (handles symbol normalization)
        cex_data = self.get_cex_prices(symbol, start_time, end_time)
        if not cex_data:
            logger.warning(f"No CEX data for {symbol}")
            return None
        
        cex_prices = cex_data['prices']
        cex_times = cex_data['times']
        
        logger.info(f"CEX prices type: {type(cex_prices)}, length: {len(cex_prices) if hasattr(cex_prices, '__len__') else 'N/A'}")
        
        # Calculate CEX volatility
        cex_vol = np.std(np.diff(cex_prices) / np.array(cex_prices[:-1])) if len(cex_prices) > 1 else 0
        
        # Parse symbol and find DEX pool
        if '/' not in symbol:
            logger.error(f"Symbol must be in format BASE/QUOTE (e.g., BTC/USDT)")
            return None
        
        base, quote = symbol.split('/')
        
        # Find DEX pool with matching tokens directly from dex_swaps
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ds.pool_address, COUNT(*) as swap_count
            FROM dex_swaps ds
            LEFT JOIN tokens t_in ON ds.token_in = t_in.address
            LEFT JOIN tokens t_out ON ds.token_out = t_out.address
            WHERE (
                (t_in.symbol IN (%s, %s) AND t_out.symbol = %s)
                OR (t_out.symbol IN (%s, %s) AND t_in.symbol = %s)
            )
            AND ds.time >= %s
            AND ds.time <= %s
            GROUP BY ds.pool_address
            ORDER BY swap_count DESC
            LIMIT 1
        """, (
            base.upper(), f"W{base.upper()}", quote.upper(),
            base.upper(), f"W{base.upper()}", quote.upper(),
            start_time, end_time
        ))
        
        pool_row = cursor.fetchone()
        cursor.close()
        
        if not pool_row:
            logger.warning(f"No DEX pool found for {symbol}")
            # Return partial result with CEX data only
            return CorrelationResult(
                symbol=symbol,
                period=f"{hours}h",
                cex_dex_correlation=0.0,
                dex_leading=False,
                lead_lag_periods=0,
                lead_lag_seconds=0.0,
                cex_volatility=float(cex_vol),
                dex_volatility=0.0,
                price_deviation_mean=0.0,
                price_deviation_std=0.0
            )
        
        pool_address = pool_row[0]
        logger.info(f"Analyzing pool {pool_address[:10]}... for {symbol}")
        
        # Get token addresses for this pool
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT token_in, token_out
            FROM dex_swaps
            WHERE pool_address = %s
            LIMIT 1
        """, (pool_address,))
        
        token_row = cursor.fetchone()
        cursor.close()
        
        if not token_row:
            logger.warning(f"No tokens found for pool {pool_address}")
            return CorrelationResult(
                symbol=symbol,
                period=f"{hours}h",
                cex_dex_correlation=0.0,
                dex_leading=False,
                lead_lag_periods=0,
                lead_lag_seconds=0.0,
                cex_volatility=float(cex_vol),
                dex_volatility=0.0,
                price_deviation_mean=0.0,
                price_deviation_std=0.0
            )
        
        token_pair = (token_row[0], token_row[1])
        
        # Determine chain based on pool
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT chain
            FROM dex_swaps
            WHERE pool_address = %s
            LIMIT 1
        """, (pool_address,))
        chain_row = cursor.fetchone()
        cursor.close()
        chain = chain_row[0] if chain_row else "ethereum"
        
        # Get DEX prices
        dex_data = self.get_dex_prices(token_pair, start_time, end_time, chain)
        if not dex_data or len(dex_data.get('prices', [])) < 2:
            logger.warning(f"Insufficient DEX data for pool {pool_address}")
            return CorrelationResult(
                symbol=symbol,
                period=f"{hours}h",
                cex_dex_correlation=0.0,
                dex_leading=False,
                lead_lag_periods=0,
                lead_lag_seconds=0.0,
                cex_volatility=float(cex_vol),
                dex_volatility=0.0,
                price_deviation_mean=0.0,
                price_deviation_std=0.0
            )
        
        dex_prices = dex_data['prices']
        
        logger.info(f"DEX prices type: {type(dex_prices)}, length: {len(dex_prices) if hasattr(dex_prices, '__len__') else 'N/A'}")
        
        # Calculate DEX volatility
        dex_vol = np.std(np.diff(dex_prices) / np.array(dex_prices[:-1])) if len(dex_prices) > 1 else 0
        
        # Calculate correlation and lead-lag
        if len(cex_prices) >= 10 and len(dex_prices) >= 10:
            # Resample to common timeline
            min_len = min(len(cex_prices), len(dex_prices))
            cex_sample = np.array(cex_prices[:min_len], dtype=float)
            dex_sample = np.array(dex_prices[:min_len], dtype=float)
            
            # Calculate correlation (stack as rows)
            if min_len > 1:
                correlation = np.corrcoef(np.vstack([cex_sample, dex_sample]))[0, 1]
            else:
                correlation = 0.0
            lead_lag_periods, max_corr = self.calculate_lead_lag(list(cex_sample), list(dex_sample))
            
            # Calculate price deviations
            deviations = [(d - c) / c * 100 for c, d in zip(cex_sample, dex_sample) if c > 0]
            dev_mean = np.mean(deviations) if deviations else 0.0
            dev_std = np.std(deviations) if deviations else 0.0
        else:
            correlation = 0.0
            lead_lag_periods = 0
            dev_mean = 0.0
            dev_std = 0.0
        
        result = CorrelationResult(
            symbol=symbol,
            period=f"{hours}h",
            cex_dex_correlation=float(correlation),
            dex_leading=lead_lag_periods < 0,
            lead_lag_periods=abs(lead_lag_periods),
            lead_lag_seconds=float(abs(lead_lag_periods) * 60),  # Assuming 60s intervals
            cex_volatility=float(cex_vol),
            dex_volatility=float(dex_vol),
            price_deviation_mean=float(dev_mean),
            price_deviation_std=float(dev_std)
        )
        
        # Save to database
        self.save_correlation(result)
        
        return result
    
    def save_correlation(self, result: CorrelationResult) -> bool:
        """Save correlation result to database"""
        try:
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                INSERT INTO correlation_analysis 
                (symbol, period, cex_dex_correlation, dex_leading, lead_lag_periods,
                 lead_lag_seconds, cex_volatility, dex_volatility, price_deviation_mean,
                 price_deviation_std)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                result.symbol,
                result.period,
                result.cex_dex_correlation,
                result.dex_leading,
                result.lead_lag_periods,
                result.lead_lag_seconds,
                result.cex_volatility,
                result.dex_volatility,
                result.price_deviation_mean,
                result.price_deviation_std
            ))
            
            self.db.conn.commit()
            cursor.close()
            
            logger.info(f"Saved correlation for {result.symbol}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving correlation: {e}")
            self.db.conn.rollback()
            return False
    
    def close(self):
        """Close database connection"""
        self.db.close()


class SlippageAnalyzer:
    """Analyzes slippage and price deviations"""
    
    def __init__(self):
        self.db = get_database_client()
        logger.info("Initialized SlippageAnalyzer")
    
    def calculate_slippage(self, amount_in: float, amount_out: float,
                          spot_price: float) -> float:
        """
        Calculate slippage percentage
        
        Args:
            amount_in: Amount swapped in
            amount_out: Amount received
            spot_price: Expected spot price
        
        Returns:
            Slippage percentage
        """
        if amount_in == 0:
            return 0.0
        
        expected_out = amount_in * spot_price
        if expected_out == 0:
            return 0.0
        
        slippage = (expected_out - amount_out) / expected_out * 100
        return max(0, float(slippage))  # Slippage >= 0
    
    def analyze_pool(self, pool_address: str, hours: int = 24) -> Dict:
        """
        Analyze slippage in a specific pool
        
        Args:
            pool_address: Pool contract address
            hours: Historical period
        
        Returns:
            Dictionary with slippage statistics
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)
            
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT amount_in, amount_out, price
                FROM dex_swaps
                WHERE pool_address = %s
                  AND time >= %s
                  AND time <= %s
                ORDER BY time ASC
            """, (pool_address, start_time, end_time))
            
            rows = cursor.fetchall()
            cursor.close()
            
            if not rows:
                return {'pool': pool_address, 'swaps': 0}
            
            slippages = []
            for amount_in, amount_out, price in rows:
                slip = self.calculate_slippage(float(amount_in), float(amount_out), float(price))
                slippages.append(slip)
            
            return {
                'pool': pool_address,
                'swaps': len(slippages),
                'mean_slippage': float(np.mean(slippages)),
                'median_slippage': float(np.median(slippages)),
                'std_slippage': float(np.std(slippages)),
                'max_slippage': float(np.max(slippages)),
                'min_slippage': float(np.min(slippages))
            }
        
        except Exception as e:
            logger.error(f"Error analyzing pool {pool_address}: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Example usage
if __name__ == "__main__":
    analyzer = LeadLagAnalyzer()
    
    # Analyze BTC/USDT
    result = analyzer.analyze_symbol("BTC/USDT", hours=24)
    if result:
        logger.info(f"Symbol: {result.symbol}")
        logger.info(f"CEX Volatility: {result.cex_volatility:.4f}")
        logger.info(f"Correlation: {result.cex_dex_correlation:.4f}")
        analyzer.save_correlation(result)
    
    # Analyze slippage
    slippage_analyzer = SlippageAnalyzer()
    pool_stats = slippage_analyzer.analyze_pool("0x1f9840a85d5af5bf1d1762f925bdaddc4201f984")
    logger.info(f"Pool Stats: {json.dumps(pool_stats, indent=2)}")
    
    analyzer.close()
    slippage_analyzer.close()
    logger.info("Analysis complete!")
