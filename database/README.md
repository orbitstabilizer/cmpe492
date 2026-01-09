# Database Infrastructure

PostgreSQL + TimescaleDB database for CMPE 492 crypto exchange analysis.

## Quick Start

### 1. Start PostgreSQL Container

```bash
cd database
./start-db.sh
```

Or directly with docker-compose:

```bash
cd database
docker-compose up -d
```

### 2. Verify Setup

```bash
# Check containers
docker-compose ps

# Access Adminer (web UI)
# Open http://localhost:8080 in your browser
# Login: Server=postgres, User=cmpe492, Password=password123
```

## Structure

```
database/
├── docker-compose.yml      # PostgreSQL + Adminer setup
├── init-db.sql             # Database schema initialization
├── start-db.sh             # Startup script
├── .env.database           # Environment variables
├── db_client.py            # PostgreSQL client
├── db_init.py              # Database initialization utilities
├── pyproject.toml          # Dependencies (use uv)
└── README.md               # This file
```

## Python Usage

Install dependencies:

```bash
cd database
uv sync
```

Use in your code:

```python
from db_init import get_db

# Get database connection
db = get_db()

# Insert CEX ticker data
tickers = [
    {
        'time': datetime.now(),
        'exchange': 'binance',
        'symbol': 'BTC/USDT',
        'bid': 42500.50,
        'ask': 42501.50
    }
]
db.insert_cex_ticker(tickers)

# Query prices
price = db.get_latest_cex_price('BTC/USDT')
print(f"BTC price: {price['mid_price']}")
```

## Database Schema

### Time-Series Tables (TimescaleDB Hypertables)

- **cex_tickers** - CEX ticker data (bid/ask)
- **price_index** - Aggregated price index from CEX sources
- **dex_swaps** - DEX swap events
- **price_deviations** - CEX vs DEX price analysis
- **slippage_analysis** - Slippage modeling results

### Metadata Tables

- **tokens** - Token metadata
- **pools** - Pool/market information
- **correlation_analysis** - Lead-lag correlation results
- **data_ingestion_logs** - Data collection monitoring

## Configuration

Edit `.env.database`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crypto_exchange
DB_USER=cmpe492
DB_PASSWORD=password123
```

## Connection Details

- **Host**: localhost:5432
- **Database**: crypto_exchange
- **User**: cmpe492
- **Password**: password123

## Useful Commands

```bash
# Check status
docker-compose ps

# View logs
docker-compose logs -f postgres

# Stop containers
docker-compose stop

# Stop and remove
docker-compose down

# Remove all data (danger!)
docker-compose down -v

# Database backup
docker-compose exec postgres pg_dump -U cmpe492 crypto_exchange > backup.sql

# Database restore
cat backup.sql | docker-compose exec -T postgres psql -U cmpe492 -d crypto_exchange
```

## Web Admin Interface

Access Adminer at http://localhost:8080

- **System**: PostgreSQL
- **Server**: postgres
- **Username**: cmpe492
- **Password**: password123
- **Database**: crypto_exchange

## Integration with Services

### From price-index (Go)

```go
import "github.com/lib/pq"

// After calculating price index, insert to database
err := db.InsertPriceIndex(priceIndices)
```

### From uniswap-eth-listener (Python)

```python
from db_init import get_db

db = get_db()
db.insert_dex_swaps(swap_events)
```

### From dex-prices (Go)

Similar pattern - use PostgreSQL driver to insert swap events.
