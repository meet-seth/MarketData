STOCKS_FILE = "stocks.txt"

with open(STOCKS_FILE) as f:
    STOCKS_TO_TRACK = list(map(str.strip,f.readlines()))

CONSTANT_DATABASE_NAME = "market_data"

CONSTANT_TIMESTAMP_COLLECTION = "last_timestamp"

CONSTANT_TIME_INTERVAL = "1m"
CONSTANT_MAX_SIZE_LIMIT = 30
CONSTANT_DAYS_PER_REQUEST = 7