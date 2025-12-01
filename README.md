# BTCUSDT 1-Minute Kline Data → TimescaleDB

Import Binance BTCUSDT 1-minute historical kline data into a TimescaleDB time series database.

## Data

The `data/btcusdt_1m/` folder contains daily CSV files from Binance with 1-minute OHLCV candlestick data dating back to August 2017.

Each CSV row contains:
| Column | Description |
|--------|-------------|
| open_time | Candle open timestamp (ms) |
| open | Opening price |
| high | Highest price |
| low | Lowest price |
| close | Closing price |
| volume | Base asset volume |
| close_time | Candle close timestamp (ms) |
| quote_asset_volume | Quote asset volume |
| number_of_trades | Number of trades |
| taker_buy_base_volume | Taker buy base volume |
| taker_buy_quote_volume | Taker buy quote volume |

## Prerequisites

- Python 3.8+
- PostgreSQL with [TimescaleDB](https://docs.timescale.com/install/) extension

### macOS (Homebrew)

```bash
brew install postgresql@17 timescaledb
```

Add to `/opt/homebrew/var/postgresql@17/postgresql.conf`:
```
shared_preload_libraries = 'timescaledb'
```

Restart PostgreSQL:
```bash
brew services restart postgresql@17
```

## Setup

1. **Create the database:**
   ```bash
   createdb crypto
   psql -d crypto -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the import script:**
   
   Edit `import_klines.py` and update `DB_CONFIG` with your credentials:
   ```python
   DB_CONFIG = {
       "host": "localhost",
       "port": 5432,
       "database": "crypto",
       "user": "your_username",
       "password": "",
   }
   ```

4. **Run the import:**
   ```bash
   python import_klines.py
   ```

## Usage

Once imported, query your data:

```sql
-- Latest candles
SELECT * FROM btcusdt_1m ORDER BY open_time DESC LIMIT 10;

-- Aggregate to hourly OHLCV
SELECT 
    time_bucket('1 hour', open_time) AS bucket,
    first(open, open_time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, open_time) AS close,
    sum(volume) AS volume
FROM btcusdt_1m
GROUP BY bucket
ORDER BY bucket DESC
LIMIT 24;

-- Daily summary
SELECT 
    time_bucket('1 day', open_time) AS day,
    first(open, open_time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, open_time) AS close,
    sum(volume) AS volume,
    sum(number_of_trades) AS trades
FROM btcusdt_1m
GROUP BY day
ORDER BY day DESC;
```

## Data Source

Historical kline data from [Binance Public Data](https://data.binance.vision/).

