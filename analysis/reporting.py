"""
Report Generation Module
Generates analysis reports and statistics from collected data
"""

import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import asdict
import sys

try:
    from database import get_database_client
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from database import get_database_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataQualityReport:
    """Analyzes and reports on data quality"""
    
    def __init__(self):
        self.db = get_database_client()
        logger.info("✅ Initialized DataQualityReport")
    
    def get_data_coverage(self) -> Dict:
        """Get data coverage statistics"""
        try:
            cursor = self.db.conn.cursor()
            
            # CEX data
            cursor.execute("""
                SELECT 
                    COUNT(*) as cex_records,
                    COUNT(DISTINCT symbol) as cex_symbols,
                    MIN(time) as cex_start,
                    MAX(time) as cex_end
                FROM price_index
            """)
            cex_stats = cursor.fetchone()
            
            # DEX data
            cursor.execute("""
                SELECT 
                    COUNT(*) as dex_records,
                    COUNT(DISTINCT chain) as chains,
                    COUNT(DISTINCT dex) as dex_protocols,
                    MIN(time) as dex_start,
                    MAX(time) as dex_end
                FROM dex_swaps
            """)
            dex_stats = cursor.fetchone()
            
            cursor.close()
            
            return {
                'cex': {
                    'records': cex_stats[0] if cex_stats else 0,
                    'symbols': cex_stats[1] if cex_stats else 0,
                    'start_time': cex_stats[2].isoformat() if cex_stats and cex_stats[2] else None,
                    'end_time': cex_stats[3].isoformat() if cex_stats and cex_stats[3] else None
                },
                'dex': {
                    'records': dex_stats[0] if dex_stats else 0,
                    'chains': dex_stats[1] if dex_stats else 0,
                    'protocols': dex_stats[2] if dex_stats else 0,
                    'start_time': dex_stats[3].isoformat() if dex_stats and dex_stats[3] else None,
                    'end_time': dex_stats[4].isoformat() if dex_stats and dex_stats[4] else None
                }
            }
        
        except Exception as e:
            logger.error(f"Error getting data coverage: {e}")
            return {}
    
    def get_ingestion_summary(self, hours: int = 24) -> Dict:
        """Get data ingestion summary for recent period"""
        try:
            cursor = self.db.conn.cursor()
            
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            cursor.execute("""
                SELECT 
                    service_name,
                    event_type,
                    status,
                    COUNT(*) as count,
                    SUM(records_processed) as total_records
                FROM data_ingestion_logs
                WHERE time >= %s
                GROUP BY service_name, event_type, status
                ORDER BY time DESC
            """, (cutoff_time,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            summary = {}
            for service, event_type, status, count, records in rows:
                if service not in summary:
                    summary[service] = {}
                if event_type not in summary[service]:
                    summary[service][event_type] = {}
                
                summary[service][event_type][status] = {
                    'events': count,
                    'records': records or 0
                }
            
            return summary
        
        except Exception as e:
            logger.error(f"Error getting ingestion summary: {e}")
            return {}
    
    def generate_report(self) -> Dict:
        """Generate comprehensive quality report"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'data_coverage': self.get_data_coverage(),
            'recent_ingestion': self.get_ingestion_summary(hours=24)
        }
    
    def close(self):
        """Close database connection"""
        self.db.close()


class StatisticsReport:
    """Generates statistical analysis reports"""
    
    def __init__(self):
        self.db = get_database_client()
        logger.info("✅ Initialized StatisticsReport")
    
    def get_cex_statistics(self, symbol: str, hours: int = 24) -> Dict:
        """Get CEX price statistics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)
            
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    symbol,
                    COUNT(*) as samples,
                    AVG(price) as mean_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    STDDEV(price) as std_dev,
                    AVG(std_dev) as mean_spread
                FROM price_index
                WHERE symbol = %s
                  AND time >= %s
                  AND time <= %s
                GROUP BY symbol
            """, (symbol, start_time, end_time))
            
            row = cursor.fetchone()
            cursor.close()
            
            if not row:
                return {}
            
            return {
                'symbol': row[0],
                'samples': row[1],
                'mean_price': float(row[2]) if row[2] else 0,
                'min_price': float(row[3]) if row[3] else 0,
                'max_price': float(row[4]) if row[4] else 0,
                'std_dev': float(row[5]) if row[5] else 0,
                'mean_spread': float(row[6]) if row[6] else 0,
                'period_hours': hours
            }
        
        except Exception as e:
            logger.error(f"Error getting CEX statistics: {e}")
            return {}
    
    def get_dex_statistics(self, hours: int = 24) -> Dict:
        """Get DEX pool statistics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)
            
            cursor = self.db.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    pool_address,
                    dex,
                    chain,
                    COUNT(*) as swaps,
                    AVG(price) as mean_price,
                    AVG(amount_in) as mean_amount_in,
                    AVG(amount_out) as mean_amount_out,
                    SUM(amount_in) as total_volume
                FROM dex_swaps
                WHERE time >= %s
                  AND time <= %s
                GROUP BY pool_address, dex, chain
                ORDER BY total_volume DESC
                LIMIT 10
            """, (start_time, end_time))
            
            rows = cursor.fetchall()
            cursor.close()
            
            pools = []
            for row in rows:
                pools.append({
                    'pool_address': row[0],
                    'dex': row[1],
                    'chain': row[2],
                    'swaps': row[3],
                    'mean_price': float(row[4]) if row[4] else 0,
                    'mean_amount_in': float(row[5]) if row[5] else 0,
                    'mean_amount_out': float(row[6]) if row[6] else 0,
                    'total_volume': float(row[7]) if row[7] else 0
                })
            
            return {
                'period_hours': hours,
                'top_pools': pools
            }
        
        except Exception as e:
            logger.error(f"Error getting DEX statistics: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        self.db.close()


class ResultsGenerator:
    """Generates final results for report"""
    
    def __init__(self):
        self.db = get_database_client()
        self.quality = DataQualityReport()
        self.stats = StatisticsReport()
        logger.info("✅ Initialized ResultsGenerator")
    
    def generate_section_data(self) -> Dict:
        """Generate all data needed for report Results section"""
        return {
            'generated_at': datetime.utcnow().isoformat(),
            'data_quality': self.quality.generate_report(),
            'cex_analysis': {
                'BTC/USDT': self.stats.get_cex_statistics('BTC/USDT', 24),
                'ETH/USDT': self.stats.get_cex_statistics('ETH/USDT', 24)
            },
            'dex_analysis': self.stats.get_dex_statistics(24),
            'summary': {
                'total_symbols_analyzed': 2,
                'data_collection_running': True,
                'next_analysis_phase': 'correlation and lead-lag detection'
            }
        }
    
    def export_json(self, output_path: str) -> bool:
        """Export results as JSON"""
        try:
            results = self.generate_section_data()
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            
            logger.info(f"✅ Exported results to {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error exporting JSON: {e}")
            return False
    
    def close(self):
        """Close all connections"""
        self.quality.close()
        self.stats.close()
        self.db.close()


# Example usage
if __name__ == "__main__":
    generator = ResultsGenerator()
    
    # Generate and display results
    results = generator.generate_section_data()
    logger.info(f"Results:\n{json.dumps(results, indent=2)}")
    
    # Export to file
    generator.export_json('/tmp/cmpe492_results.json')
    
    generator.close()
    logger.info("✅ Report generation complete!")
