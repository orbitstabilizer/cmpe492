#!/usr/bin/env python3
"""
Run Analysis Script
Executes various analyses on collected data
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.deviation_calculator import PriceDeviationCalculator
from analysis.correlation import LeadLagAnalyzer
from analysis.slippage import SlippageAnalyzer
from analysis.volume_analysis import VolumeAnalyzer
from analysis.reporting import DataQualityReport, StatisticsReport

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_data_quality_check():
    """Check data quality and coverage"""
    logger.info("=" * 60)
    logger.info("DATA QUALITY CHECK")
    logger.info("=" * 60)
    
    report = DataQualityReport()
    coverage = report.get_data_coverage()
    
    logger.info("\nCEX Data:")
    logger.info(f"  Records: {coverage['cex']['records']:,}")
    logger.info(f"  Symbols: {coverage['cex']['symbols']}")
    logger.info(f"  Start: {coverage['cex']['start_time']}")
    logger.info(f"  End: {coverage['cex']['end_time']}")
    
    logger.info("\nDEX Data:")
    logger.info(f"  Records: {coverage['dex']['records']:,}")
    logger.info(f"  Chains: {coverage['dex']['chains']}")
    logger.info(f"  Protocols: {coverage['dex']['protocols']}")
    logger.info(f"  Start: {coverage['dex']['start_time']}")
    logger.info(f"  End: {coverage['dex']['end_time']}")
    
    report.close()
    logger.info("\n✅ Data quality check complete")


def run_deviation_analysis(pool_address: str, hours: int = 24):
    """Run price deviation analysis"""
    logger.info("=" * 60)
    logger.info("PRICE DEVIATION ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Pool: {pool_address}")
    logger.info(f"Period: {hours} hours")
    
    calculator = PriceDeviationCalculator()
    stats = calculator.analyze_pool(pool_address, hours)
    
    if stats:
        logger.info(f"\n📊 Results for {stats.symbol}:")
        logger.info(f"  Sample Size: {stats.sample_size:,}")
        logger.info(f"  Mean Deviation: {stats.mean_deviation:.4f}%")
        logger.info(f"  Median Deviation: {stats.median_deviation:.4f}%")
        logger.info(f"  Std Deviation: {stats.std_deviation:.4f}%")
        logger.info(f"  P90 Deviation: {stats.p90_deviation:.4f}%")
        logger.info(f"  P95 Deviation: {stats.p95_deviation:.4f}%")
        logger.info(f"  P99 Deviation: {stats.p99_deviation:.4f}%")
        logger.info(f"  Max Deviation: {stats.max_deviation:.4f}%")
        logger.info(f"  Min Deviation: {stats.min_deviation:.4f}%")
        logger.info(f"  Arbitrage Opportunities: {stats.arbitrage_opportunities}")
    else:
        logger.warning("❌ No data available for analysis")
    
    calculator.close()
    logger.info("\n✅ Deviation analysis complete")


def run_correlation_analysis(symbol: str, hours: int = 24):
    """Run lead-lag correlation analysis"""
    logger.info("=" * 60)
    logger.info("LEAD-LAG CORRELATION ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Period: {hours} hours")
    
    analyzer = LeadLagAnalyzer()
    result = analyzer.analyze_symbol(symbol, hours)
    
    if result:
        logger.info(f"\n📊 Results for {result.symbol}:")
        logger.info(f"  Correlation: {result.cex_dex_correlation:.4f}")
        logger.info(f"  Lead/Lag: {result.lead_lag_seconds:.2f} seconds")
        logger.info(f"  DEX Leading: {result.dex_leading}")
        logger.info(f"  CEX Volatility: {result.cex_volatility:.4f}")
        logger.info(f"  DEX Volatility: {result.dex_volatility:.4f}")
        logger.info(f"  Mean Deviation: {result.price_deviation_mean:.4f}%")
    else:
        logger.warning("❌ No data available for analysis")
    
    analyzer.close()
    logger.info("\n✅ Correlation analysis complete")


def run_statistics_report(symbol: str, hours: int = 24):
    """Run statistics report"""
    logger.info("=" * 60)
    logger.info("STATISTICS REPORT")
    logger.info("=" * 60)
    
    report = StatisticsReport()
    
    # CEX statistics
    cex_stats = report.get_cex_statistics(symbol, hours)
    if cex_stats:
        logger.info(f"\nCEX Statistics for {symbol}:")
        logger.info(f"  Samples: {cex_stats['samples']:,}")
        logger.info(f"  Mean Price: ${cex_stats['mean_price']:,.2f}")
        logger.info(f"  Min Price: ${cex_stats['min_price']:,.2f}")
        logger.info(f"  Max Price: ${cex_stats['max_price']:,.2f}")
        logger.info(f"  Std Dev: ${cex_stats['std_dev']:,.2f}")
    
    # DEX statistics
    dex_stats = report.get_dex_statistics(hours)
    if dex_stats and dex_stats['top_pools']:
        logger.info(f"\nTop DEX Pools:")
        for i, pool in enumerate(dex_stats['top_pools'][:5], 1):
            logger.info(f"  {i}. {pool['pool_address'][:10]}... ({pool['dex']})")
            logger.info(f"     Swaps: {pool['swaps']:,}, Volume: ${pool['total_volume']:,.2f}")
    
    report.close()
    logger.info("\n✅ Statistics report complete")


def run_volume_analysis(symbol: str, hours: int = 24):
    """Run volume distribution analysis"""
    logger.info("=" * 60)
    logger.info("VOLUME DISTRIBUTION ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Symbol: {symbol}")
    logger.info(f"Period: {hours} hours")
    
    analyzer = VolumeAnalyzer()
    
    # Volume distribution
    distribution = analyzer.analyze_volume_distribution(symbol, hours)
    if distribution:
        logger.info(f"\n📊 Volume Distribution for {distribution.symbol}:")
        logger.info(f"  Total CEX Volume: ${distribution.total_cex_volume:,.2f} ({distribution.cex_percentage:.1f}%)")
        logger.info(f"  Total DEX Volume: ${distribution.total_dex_volume:,.2f} ({distribution.dex_percentage:.1f}%)")
        
        if distribution.cex_by_exchange:
            logger.info(f"\n  CEX by Exchange:")
            for exchange, vol in sorted(distribution.cex_by_exchange.items(), key=lambda x: x[1], reverse=True):
                pct = (vol / distribution.total_cex_volume * 100) if distribution.total_cex_volume > 0 else 0
                logger.info(f"    {exchange}: ${vol:,.2f} ({pct:.1f}%)")
        
        if distribution.dex_by_protocol:
            logger.info(f"\n  DEX by Protocol:")
            for protocol, vol in sorted(distribution.dex_by_protocol.items(), key=lambda x: x[1], reverse=True):
                pct = (vol / distribution.total_dex_volume * 100) if distribution.total_dex_volume > 0 else 0
                logger.info(f"    {protocol}: ${vol:,.2f} ({pct:.1f}%)")
    
    # Market concentration
    concentration = analyzer.get_volume_concentration(hours)
    if concentration:
        logger.info(f"\n📈 Market Concentration:")
        logger.info(f"  CEX HHI: {concentration['cex_herfindahl_index']:.0f} ({concentration['cex_concentration']})")
        logger.info(f"  DEX HHI: {concentration['dex_herfindahl_index']:.0f} ({concentration['dex_concentration']})")
    
    analyzer.close()
    logger.info("\n✅ Volume analysis complete")


def main():
    parser = argparse.ArgumentParser(
        description='Run analysis on collected CEX/DEX data'
    )
    
    parser.add_argument(
        'analysis_type',
        choices=['quality', 'deviation', 'correlation', 'statistics', 'volume', 'all'],
        help='Type of analysis to run'
    )
    
    parser.add_argument(
        '--symbol',
        default='BTC/USDT',
        help='Trading pair symbol (default: BTC/USDT)'
    )
    
    parser.add_argument(
        '--pool',
        help='DEX pool address for deviation analysis'
    )
    
    parser.add_argument(
        '--hours',
        type=int,
        default=24,
        help='Hours of data to analyze (default: 24)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.analysis_type == 'quality' or args.analysis_type == 'all':
            run_data_quality_check()
            print()
        
        if args.analysis_type == 'deviation' or args.analysis_type == 'all':
            if not args.pool and args.analysis_type == 'deviation':
                logger.error("❌ --pool required for deviation analysis")
                return 1
            elif args.pool:
                run_deviation_analysis(args.pool, args.hours)
                print()
        
        if args.analysis_type == 'correlation' or args.analysis_type == 'all':
            run_correlation_analysis(args.symbol, args.hours)
            print()
        
        if args.analysis_type == 'statistics' or args.analysis_type == 'all':
            run_statistics_report(args.symbol, args.hours)
            print()
        
        if args.analysis_type == 'volume' or args.analysis_type == 'all':
            run_volume_analysis(args.symbol, args.hours)
            print()
        
        logger.info("=" * 60)
        logger.info("✅ ALL ANALYSES COMPLETE")
        logger.info("=" * 60)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

