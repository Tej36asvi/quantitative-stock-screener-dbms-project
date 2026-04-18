import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import time

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'quant_screener_dbms'
}

STOCKS = [
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
    'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS'
]

END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=15*365)

def get_stock_id(cursor, symbol):
    cursor.execute("SELECT stock_id FROM stocks WHERE symbol = %s", (symbol,))
    result = cursor.fetchone()
    return result[0] if result else None

def download_stock_data(symbol):
    print(f"Downloading {symbol}...")
    ticker = yf.Ticker(symbol)
    df = ticker.history(start=START_DATE, end=END_DATE, auto_adjust=False)
    
    if df.empty:
        return None
    
    df.reset_index(inplace=True)
    df['trade_date'] = pd.to_datetime(df['Date']).dt.date
    df['adj_close'] = df['Close']
    
    df = df[['trade_date', 'Open', 'High', 'Low', 'Close', 'adj_close', 'Volume']].copy()
    df.columns = ['trade_date', 'open_price', 'high_price', 'low_price', 
                  'close_price', 'adj_close', 'volume']
    
    df = df[(df['open_price'] > 0) & (df['high_price'] >= df['low_price'])].copy()
    df['volume'] = df['volume'].fillna(0).astype(int)
    
    print(f"  {len(df)} records from {df['trade_date'].min()} to {df['trade_date'].max()}")
    return df

def insert_price_data(connection, stock_id, df):
    cursor = connection.cursor()
    
    query = """
        INSERT INTO daily_prices 
            (stock_id, trade_date, open_price, high_price, low_price, 
             close_price, adj_close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_price = VALUES(open_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            close_price = VALUES(close_price),
            adj_close = VALUES(adj_close),
            volume = VALUES(volume)
    """
    
    records = [
        (int(stock_id), row['trade_date'], float(row['open_price']),
         float(row['high_price']), float(row['low_price']), 
         float(row['close_price']), float(row['adj_close']), int(row['volume']))
        for _, row in df.iterrows()
    ]
    
    cursor.executemany(query, records)
    connection.commit()
    cursor.close()
    print(f"  Inserted {len(records)} records")
    return len(records)

def load_all_stocks():
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    
    total_records = 0
    print(f"\nLoading {len(STOCKS)} stocks from {START_DATE.date()} to {END_DATE.date()}\n")
    
    for symbol in STOCKS:
        stock_id = get_stock_id(cursor, symbol)
        if not stock_id:
            print(f"ERROR: {symbol} not in database")
            continue
        
        df = download_stock_data(symbol)
        if df is not None and len(df) > 0:
            total_records += insert_price_data(connection, stock_id, df)
        
        time.sleep(1)
    
    cursor.close()
    connection.close()
    
    print(f"\nTotal records inserted: {total_records:,}")
    verify_database()

def verify_database():
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT 
            s.symbol,
            COUNT(dp.price_id) as records,
            MIN(dp.trade_date) as from_date,
            MAX(dp.trade_date) as to_date
        FROM stocks s
        LEFT JOIN daily_prices dp ON s.stock_id = dp.stock_id
        GROUP BY s.stock_id, s.symbol
        ORDER BY s.symbol
    """)
    
    print("\nDatabase Summary:")
    for row in cursor.fetchall():
        print(f"{row['symbol']:15} {row['records']:6,} records  {row['from_date']} to {row['to_date']}")
    
    cursor.close()
    connection.close()

if __name__ == '__main__':
    load_all_stocks()
    
    print("\nCalculating indicators...")
    connection = mysql.connector.connect(**DB_CONFIG)
    
    cursor = connection.cursor()
    cursor.callproc('sp_calculate_momentum')
    cursor.close()
    
    cursor = connection.cursor()
    cursor.callproc('sp_calculate_pairs')
    cursor.close()
    
    connection.commit()
    connection.close()
    
    print("Indicators calculated")
    print("\nData load complete")