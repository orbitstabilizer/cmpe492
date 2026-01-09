# Database Setup for CMPE 492 Crypto Exchange Analysis

This directory contains the database infrastructure for collecting and analyzing CEX and DEX market data.

## Architecture

- **PostgreSQL 16** with **TimescaleDB** extension
  - Time-series optimized database for cryptocurrency market data
  - Hypertables for automatic partitioning and compression
  - Excellent performance for high-volume tick data

- **Adminer** for web-based database administration
  - No installation needed, works in browser
  - View tables, run queries, manage data

## Quick Start

### 1. Start the Database

```bash
# Make script executable
chmod +x start-db.sh

# Run the startup script
./start-db.sh
```

Or use docker-compose directly:

```bash
docker-compose up -d
```

### 2. Verify Setup

```bash
# Check container status
docker-compose ps

# Test connection with psql
psql -h localhost -U cmpe492 -d crypto_exchange

# Or use Adminer web interface
# Open http://localhost:8080 in your browser
```

## Database Schema

### Core Tables

#### `cex_tickers` (Time-series)
Stores CEX ticker data (bid/ask prices) from all exchanges.

```sql
SELECT * FROM cex_tickers 
WHERE symbol = 'BTC/USDT' 
AND time > now() - interval '1 hour'
ORDER BY time DESC;
```

#### `dex_swaps` (Time-series)
Stores DEX swap events with pricing information.

```sql
SELECT * FROM dex_swaps 
WHERE token_in = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' 
AND time > now() - interval '24 hours';
```

#### `price_index` (Time-series)
Aggregated price index calculated from CEX sources (volume-weighted average).

```sql
SELECT * FROM price_index 
WHERE symbol = 'ETH/USDT' 
ORDER BY time DESC LIMIT 100;
```

#### `price_deviations` (Time-series)
Price difference analysis between CEX and DEX.

```sql
SELECT * FROM price_deviations 
WHERE arbitrage_opportunity = TRUE 
ORDER BY time DESC;
```

#### `tokens`
Token metadata (symbols, decimals, names).

#### `pools`
DEX pool/market information (liquidity, volume, fees).

#### `correlation_analysis`
Lead-lag correlation results between CEX and DEX prices.

#### `slippage_analysis`
Slippage modeling results for different token amounts.

#### `data_ingestion_logs`
Monitoring logs for data collection services.

## Python Client Usage

```python
from db_client import CryptoExchangeDB

# Connect to database
db = CryptoExchangeDB(
    host="localhost",
    port=5432,
    database="crypto_exchange",
    user="cmpe492",
    password="password123"
)

# Insert CEX ticker data
tickers = [
    {
        'time': datetime.now(),
        'exchange': 'binance',
        'symbol': 'BTC/USDT',
        'bid': 42500.50,
        'ask': 42501.50
    },
    # ...
]
db.insert_cex_ticker(tickers)

# Get latest price
price = db.get_latest_cex_price('BTC/USDT')
print(f"Latest BTC price: {price['mid_price']}")

# Get historical prices
prices = db.get_cex_prices_range('ETH/USDT', hours=24)

# Insert DEX swaps
swaps = [
    {
        'time': datetime.now(),
        'chain': 'ethereum',
        'dex': 'uniswap-v3',
        'pool_address': '0x...',
        'token_in': '0x...',
        'token_out': '0x...',
        'amount_in': 1.0,
        'amount_out': 1800.5,
        'price': 1800.5,
        'tx_hash': '0x...',
        'block_number': 12345678
    }
]
db.insert_dex_swaps(swaps)

# Get DEX prices for a pair
dex_prices = db.get_dex_prices(
    token_in='0x...',
    token_out='0x...',
    hours=24
)

# Log ingestion event
db.log_ingestion(
    service_name='price-index',
    event_type='cex_ticker_insert',
    status='success',
    message='Inserted 8 exchanges',
    records_processed=8
)

db.close()
```

## Integration with Data Collection Services

### price-index (Go)
Modify to insert into database after calculating price index:

```go
// After calculating price index
db.insert_price_index([]struct{
    Time float64
    Symbol string
    PriceIndex float64
}{...})
```

### uniswap-eth-listener (Python)
Insert swap events directly into database:

```python
from db_client import CryptoExchangeDB

db = CryptoExchangeDB()

# After parsing swap event
db.insert_dex_swaps([{
    'time': datetime.fromtimestamp(block_timestamp),
    'chain': 'ethereum',
    'dex': 'uniswap-v3',
    # ... rest of swap data
}])
```

### dex-prices (Go)
Insert to database as events are captured.

## Maintenance

### Backup Data

```bash
# Backup database
docker-compose exec postgres pg_dump -U cmpe492 crypto_exchange > backup.sql

# Restore from backup
cat backup.sql | docker-compose exec -T postgres psql -U cmpe492 -d crypto_exchange
```

### View Logs

```bash
docker-compose logs -f postgres
docker-compose logs -f adminer
```

### Stop Services

```bash
# Stop but keep data
docker-compose stop

# Stop and remove containers (keeps data volume)
docker-compose down

# Stop and remove everything including data
docker-compose down -v
```

## Performance Tuning

For production use with high-volume data:

1. **Compression**: TimescaleDB automatically compresses old data chunks
2. **Retention**: Add policy to keep only recent data
3. **Indexing**: Already optimized for common query patterns
4. **Connection pooling**: Use pgbouncer for multiple clients

## Troubleshooting

**Container won't start:**
```bash
# Check logs
docker-compose logs postgres

# Rebuild containers
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

**Permission denied:**
```bash
# Fix with docker-compose
sudo docker-compose up -d
```

**Can't connect from Python:**
```python
# Verify connection parameters
from db_client import CryptoExchangeDB
db = CryptoExchangeDB()
print(db.health_check())  # Should return True
```

**Data growth concerns:**
```bash
# Check database size
docker-compose exec postgres psql -U cmpe492 -d crypto_exchange -c \
  "SELECT pg_size_pretty(pg_database_size('crypto_exchange'));"
```

## Resources

- [PostgreSQL Docs](https://www.postgresql.org/docs/)
- [TimescaleDB Docs](https://docs.timescale.com/)
- [Docker Docs](https://docs.docker.com/)
- [Adminer Docs](https://www.adminer.org/)
