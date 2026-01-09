"""
Volume Distribution Analysis
Analyzes trading volume distribution across CEX and DEX
"""

import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
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
class VolumeDistribution:
    """Volume distribution statistics"""
    symbol: str
    time_period: str
    total_cex_volume: float
    total_dex_volume: float
    cex_percentage: float
    dex_percentage: float
    cex_by_exchange: Dict[str, float]
    dex_by_chain: Dict[str, float]
    dex_by_protocol: Dict[str, float]


class VolumeAnalyzer:
    """Analyzes trading volume distribution"""
    
    def __init__(self):
        """Initialize volume analyzer"""
        self.db = get_database_client()
        logger.info("✅ Initialized VolumeAnalyzer")
    
    def get_cex_volume(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Get CEX volume by exchange
        
        Args:
            symbol: Trading pair symbol
            start_time: Start time
            end_time: End time
            
        Returns:
            Dictionary of exchange -> volume
        """
        try:
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    exchange,
                    SUM(base_volume) as volume
                FROM cex_tickers
                WHERE symbol = %s
                  AND time >= %s
                  AND time <= %s
                  AND base_volume IS NOT NULL
                GROUP BY exchange
            """, (symbol, start_time, end_time))
            
            rows = cursor.fetchall()
            cursor.close()
            
            result = {row[0]: float(row[1]) for row in rows}
            return result
            
        except Exception as e:
            logger.error(f"Error getting CEX volume: {e}")
            return {}
    
    def get_dex_volume(
        self,
        symbol_tokens: List[str],
        start_time: datetime,
        end_time: datetime
    ) -> Dict:
        """
        Get DEX volume by chain and protocol
        
        Args:
            symbol_tokens: List of token symbols to track
            start_time: Start time
            end_time: End time
            
        Returns:
            Dictionary with volume breakdowns
        """
        try:
            cursor = self.db.conn.cursor()
            
            # Get token addresses from symbols
            token_addresses = []
            for symbol in symbol_tokens:
                cursor.execute("""
                    SELECT address FROM tokens
                    WHERE symbol = %s
                    LIMIT 1
                """, (symbol,))
                result = cursor.fetchone()
                if result:
                    token_addresses.append(result[0])
            
            if not token_addresses:
                return {'by_chain': {}, 'by_protocol': {}, 'total': 0.0}
            
            # Volume by chain
            cursor.execute("""
                SELECT 
                    chain,
                    SUM(trade_size_usd) as volume
                FROM dex_swaps
                WHERE (token_in = ANY(%s) OR token_out = ANY(%s))
                  AND time >= %s
                  AND time <= %s
                  AND trade_size_usd IS NOT NULL
                GROUP BY chain
            """, (token_addresses, token_addresses, start_time, end_time))
            
            by_chain = {row[0]: float(row[1]) for row in cursor.fetchall()}
            
            # Volume by protocol
            cursor.execute("""
                SELECT 
                    dex,
                    SUM(trade_size_usd) as volume
                FROM dex_swaps
                WHERE (token_in = ANY(%s) OR token_out = ANY(%s))
                  AND time >= %s
                  AND time <= %s
                  AND trade_size_usd IS NOT NULL
                GROUP BY dex
            """, (token_addresses, token_addresses, start_time, end_time))
            
            by_protocol = {row[0]: float(row[1]) for row in cursor.fetchall()}
            
            cursor.close()
            
            total = sum(by_chain.values())
            
            return {
                'by_chain': by_chain,
                'by_protocol': by_protocol,
                'total': total
            }
            
        except Exception as e:
            logger.error(f"Error getting DEX volume: {e}")
            return {'by_chain': {}, 'by_protocol': {}, 'total': 0.0}
    
    def analyze_volume_distribution(
        self,
        symbol: str,
        hours: int = 24
    ) -> Optional[VolumeDistribution]:
        """
        Analyze volume distribution for a symbol
        
        Args:
            symbol: Trading pair (e.g., "BTC/USDT")
            hours: Hours to analyze
            
        Returns:
            VolumeDistribution object
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        # Parse symbol
        tokens = symbol.split('/')
        if len(tokens) != 2:
            logger.error(f"Invalid symbol format: {symbol}")
            return None
        
        # Get CEX volume
        cex_volume_by_exchange = self.get_cex_volume(symbol, start_time, end_time)
        total_cex_volume = sum(cex_volume_by_exchange.values())
        
        # Get DEX volume
        dex_data = self.get_dex_volume(tokens, start_time, end_time)
        total_dex_volume = dex_data['total']
        
        # Calculate percentages
        total_volume = total_cex_volume + total_dex_volume
        if total_volume == 0:
            logger.warning(f"No volume data found for {symbol}")
            return None
        
        cex_pct = (total_cex_volume / total_volume) * 100
        dex_pct = (total_dex_volume / total_volume) * 100
        
        result = VolumeDistribution(
            symbol=symbol,
            time_period=f"{hours}h",
            total_cex_volume=total_cex_volume,
            total_dex_volume=total_dex_volume,
            cex_percentage=cex_pct,
            dex_percentage=dex_pct,
            cex_by_exchange=cex_volume_by_exchange,
            dex_by_chain=dex_data['by_chain'],
            dex_by_protocol=dex_data['by_protocol']
        )
        
        logger.info(f"✅ Volume distribution for {symbol}: CEX {cex_pct:.1f}% / DEX {dex_pct:.1f}%")
        return result
    
    def get_trade_size_distribution(
        self,
        pool_address: str,
        hours: int = 24
    ) -> Dict:
        """
        Get trade size distribution for a pool
        
        Args:
            pool_address: Pool address
            hours: Hours to analyze
            
        Returns:
            Distribution statistics
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    trade_size_bin,
                    COUNT(*) as count,
                    SUM(trade_size_usd) as total_volume,
                    AVG(trade_size_usd) as avg_size
                FROM dex_swaps
                WHERE pool_address = %s
                  AND time >= %s
                  AND trade_size_bin IS NOT NULL
                GROUP BY trade_size_bin
            """, (pool_address, cutoff_time))
            
            rows = cursor.fetchall()
            cursor.close()
            
            distribution = {}
            for row in rows:
                size_bin = row[0]
                distribution[size_bin] = {
                    'count': row[1],
                    'total_volume': float(row[2]),
                    'avg_size': float(row[3])
                }
            
            return distribution
            
        except Exception as e:
            logger.error(f"Error getting trade size distribution: {e}")
            return {}
    
    def get_volume_concentration(
        self,
        hours: int = 24
    ) -> Dict:
        """
        Calculate volume concentration (Herfindahl index)
        
        Args:
            hours: Hours to analyze
            
        Returns:
            Concentration metrics
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            cursor = self.db.conn.cursor()
            
            # CEX concentration
            cursor.execute("""
                SELECT 
                    exchange,
                    SUM(base_volume) as volume
                FROM cex_tickers
                WHERE time >= %s
                  AND base_volume IS NOT NULL
                GROUP BY exchange
            """, (cutoff_time,))
            
            cex_volumes = [float(row[1]) for row in cursor.fetchall()]
            total_cex = sum(cex_volumes)
            
            if total_cex > 0:
                cex_shares = [(v / total_cex) ** 2 for v in cex_volumes]
                cex_hhi = sum(cex_shares) * 10000  # Scale to 0-10000
            else:
                cex_hhi = 0.0
            
            # DEX concentration
            cursor.execute("""
                SELECT 
                    dex,
                    SUM(trade_size_usd) as volume
                FROM dex_swaps
                WHERE time >= %s
                  AND trade_size_usd IS NOT NULL
                GROUP BY dex
            """, (cutoff_time,))
            
            dex_volumes = [float(row[1]) for row in cursor.fetchall()]
            total_dex = sum(dex_volumes)
            
            if total_dex > 0:
                dex_shares = [(v / total_dex) ** 2 for v in dex_volumes]
                dex_hhi = sum(dex_shares) * 10000
            else:
                dex_hhi = 0.0
            
            cursor.close()
            
            return {
                'cex_herfindahl_index': cex_hhi,
                'dex_herfindahl_index': dex_hhi,
                'cex_concentration': 'High' if cex_hhi > 2500 else 'Moderate' if cex_hhi > 1500 else 'Low',
                'dex_concentration': 'High' if dex_hhi > 2500 else 'Moderate' if dex_hhi > 1500 else 'Low'
            }
            
        except Exception as e:
            logger.error(f"Error calculating concentration: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        self.db.close()


if __name__ == "__main__":
    # Example usage
    analyzer = VolumeAnalyzer()
    
    # Example: Analyze BTC/USDT volume distribution
    # result = analyzer.analyze_volume_distribution("BTC/USDT", hours=24)
    # if result:
    #     logger.info(f"Volume Distribution for {result.symbol}:")
    #     logger.info(f"  CEX: ${result.total_cex_volume:,.2f} ({result.cex_percentage:.1f}%)")
    #     logger.info(f"  DEX: ${result.total_dex_volume:,.2f} ({result.dex_percentage:.1f}%)")
    #     logger.info(f"  CEX by Exchange: {result.cex_by_exchange}")
    #     logger.info(f"  DEX by Chain: {result.dex_by_chain}")
    
    analyzer.close()
    logger.info("✅ Done!")

