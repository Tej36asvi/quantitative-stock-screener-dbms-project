import streamlit as st
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import yfinance as yf

st.set_page_config(page_title="Stock Screener", layout="wide")

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'quant_screener_dbms'
}

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def execute_query(query):
    conn = get_db_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_database_stats():
    query = """
        SELECT 
            COUNT(DISTINCT s.stock_id) as stocks,
            COUNT(DISTINCT dp.trade_date) as days,
            MIN(dp.trade_date) as from_date,
            MAX(dp.trade_date) as to_date
        FROM stocks s
        JOIN daily_prices dp ON s.stock_id = dp.stock_id
    """
    return execute_query(query)

def update_latest_data():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT stock_id, symbol FROM stocks WHERE is_active = TRUE")
    stocks = cursor.fetchall()
    
    cursor.execute("SELECT MAX(trade_date) as last_date FROM daily_prices")
    last_date_result = cursor.fetchone()
    last_date = last_date_result['last_date']
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    if last_date and last_date >= end_date.date():
        cursor.close()
        conn.close()
        return {
            'success': False,
            'records_updated': 0,
            'errors': ['Database already has latest data'],
            'already_updated': True
        }
    
    total_updated = 0
    errors = []
    new_dates = set()
    
    for stock in stocks:
        try:
            ticker = yf.Ticker(stock['symbol'])
            df = ticker.history(start=start_date, end=end_date, auto_adjust=False)
            
            if df.empty:
                continue
            
            df.reset_index(inplace=True)
            
            for index, row in df.iterrows():
                trade_date = row['Date'].date()
                
                if trade_date <= last_date:
                    continue
                
                cursor.execute("""
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
                """, (
                    stock['stock_id'], trade_date,
                    float(row['Open']), float(row['High']), float(row['Low']),
                    float(row['Close']), float(row['Close']), int(row['Volume'])
                ))
                total_updated += 1
                new_dates.add(trade_date)
            
            conn.commit()
        except Exception as e:
            errors.append(f"{stock['symbol']}: {str(e)}")
    
    cursor.close()
    conn.close()
    
    if total_updated > 0:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.callproc('sp_calculate_momentum')
        cursor.close()
        cursor = conn.cursor()
        cursor.callproc('sp_calculate_pairs')
        cursor.close()
        conn.commit()
        conn.close()
    
    return {
        'success': total_updated > 0,
        'records_updated': total_updated,
        'new_trading_days': len(new_dates),
        'errors': errors,
        'already_updated': False
    }

MOMENTUM_QUERIES = {
    "Market Overview": {
        "description": "Current status of all 10 stocks with BUY/SELL/HOLD signals",
        "explanation": """
**How to read this table:**

Each row shows one stock's current status.

**RSI (Relative Strength Index):**
- RSI < 30 = Stock is oversold, likely to bounce back → BUY signal
- RSI 30-70 = Normal range, no extreme conditions → HOLD
- RSI > 70 = Stock is overbought, may pull back → SELL signal

**Moving Averages (Trend):**
- MA-20: 20-day average price (short-term trend)
- MA-50: 50-day average price (medium-term trend)
- MA-200: 200-day average price (long-term trend)

**Trend Column:**
- Bullish = 50-day MA is above 200-day MA (stock in uptrend)
- Bearish = 50-day MA is below 200-day MA (stock in downtrend)

**Action Column:**
- BUY - Oversold: Stock dropped too much, good entry point
- SELL - Overbought: Stock rallied too far, take profits
- HOLD - Neutral: No extreme signal, wait and watch

**How to use:**
Look for stocks with "BUY - Oversold" in a Bullish trend for best opportunities.
Avoid "SELL - Overbought" stocks in Bearish trends.
        """,
        "sql": """
            SELECT 
                s.symbol,
                s.company_name,
                ROUND(dp.close_price, 2) AS price,
                ROUND(mi.sma_20, 2) AS ma_20,
                ROUND(mi.sma_50, 2) AS ma_50,
                ROUND(mi.sma_200, 2) AS ma_200,
                ROUND(mi.rsi_14, 2) AS rsi,
                CASE 
                    WHEN mi.sma_50 > mi.sma_200 THEN 'Bullish'
                    ELSE 'Bearish'
                END AS trend,
                CASE
                    WHEN mi.rsi_14 < 30 THEN 'BUY - Oversold'
                    WHEN mi.rsi_14 > 70 THEN 'SELL - Overbought'
                    ELSE 'HOLD - Neutral'
                END AS action
            FROM daily_prices dp
            JOIN stocks s ON dp.stock_id = s.stock_id
            JOIN momentum_indicators mi ON dp.stock_id = mi.stock_id 
                AND dp.trade_date = mi.trade_date
            WHERE dp.trade_date = (SELECT MAX(trade_date) FROM daily_prices)
            ORDER BY mi.rsi_14
        """
    },
    
    "Oversold Stocks (RSI < 30)": {
        "description": "Stocks that dropped sharply - potential bounce opportunities",
        "explanation": """
**What this query shows:**
Every instance in the last 30 days where a stock's RSI dropped below 30.

**Columns explained:**
- symbol: Stock ticker (e.g., RELIANCE.NS)
- company_name: Full company name
- trade_date: When RSI went below 30
- price: Stock price on that day
- rsi: RSI value (lower = more oversold)
- days_ago: How many days ago this happened

**Why RSI < 30 matters:**
When RSI drops below 30, it means the stock has been sold off heavily and is "oversold."
Historically, oversold stocks tend to bounce back within 5-10 days.

**How to use this:**
1. Look for stocks that became oversold recently (2-5 days ago)
2. Check Market Overview to see if they're still oversold today
3. If still oversold + Bullish trend = Strong BUY signal
4. If RSI already recovered above 40 = Opportunity missed

**Risk:**
Stock may continue falling if there's fundamental bad news (earnings miss, regulatory issues, etc.)
Always check news before buying oversold stocks.
        """,
        "sql": """
            SELECT 
                s.symbol,
                s.company_name,
                mi.trade_date,
                ROUND(dp.close_price, 2) AS price,
                ROUND(mi.rsi_14, 2) AS rsi,
                DATEDIFF(CURDATE(), mi.trade_date) AS days_ago
            FROM momentum_indicators mi
            JOIN stocks s ON mi.stock_id = s.stock_id
            JOIN daily_prices dp ON mi.stock_id = dp.stock_id 
                AND mi.trade_date = dp.trade_date
            WHERE mi.rsi_14 < 30
                AND mi.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ORDER BY mi.trade_date DESC, mi.rsi_14
            LIMIT 20
        """
    },
    
    "Overbought Stocks (RSI > 70)": {
        "description": "Stocks that rallied too much - take profit signals",
        "explanation": """
**What this query shows:**
Every instance in the last 30 days where a stock's RSI exceeded 70.

**Columns explained:**
- symbol: Stock ticker
- company_name: Full company name
- trade_date: When RSI went above 70
- price: Stock price on that day
- rsi: RSI value (higher = more overbought)
- days_ago: How many days ago this happened

**Why RSI > 70 matters:**
When RSI exceeds 70, the stock has rallied strongly and is "overbought."
Overbought stocks often pull back or consolidate before moving higher.

**How to use this:**
1. If you own the stock = Consider taking partial profits
2. If you don't own it = Wait for pullback before entering
3. In very strong uptrends, stocks can stay overbought for weeks
4. Check if stock is still overbought in Market Overview

**Action:**
- RSI > 80 = Very overbought, high chance of pullback
- RSI 70-80 = Overbought, but trend may continue
- If stock already cooled down (RSI back to 60) = Pullback happened

**Exception:**
During strong bull markets, overbought can mean "strong momentum" rather than "about to fall."
Always check overall trend direction.
        """,
        "sql": """
            SELECT 
                s.symbol,
                s.company_name,
                mi.trade_date,
                ROUND(dp.close_price, 2) AS price,
                ROUND(mi.rsi_14, 2) AS rsi,
                DATEDIFF(CURDATE(), mi.trade_date) AS days_ago
            FROM momentum_indicators mi
            JOIN stocks s ON mi.stock_id = s.stock_id
            JOIN daily_prices dp ON mi.stock_id = dp.stock_id 
                AND mi.trade_date = dp.trade_date
            WHERE mi.rsi_14 > 70
                AND mi.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ORDER BY mi.trade_date DESC, mi.rsi_14 DESC
            LIMIT 20
        """
    },
    
    "Long Term Trends": {
        "description": "Golden Cross (bullish) and Death Cross (bearish) status",
        "explanation": """
**What this query shows:**
Current relationship between 50-day and 200-day moving averages for all stocks.

**Columns explained:**
- symbol: Stock ticker
- company_name: Full company name
- price: Current stock price
- ma_50: 50-day moving average
- ma_200: 200-day moving average
- strength_pct: How far apart the two MAs are
- trend_signal: Golden Cross or Death Cross

**Golden Cross:**
When 50-day MA crosses above 200-day MA = Major bullish signal
This indicates the stock has shifted from downtrend to uptrend.
Institutional investors watch for Golden Crosses to start accumulating.

**Death Cross:**
When 50-day MA crosses below 200-day MA = Major bearish signal
This indicates the stock has shifted from uptrend to downtrend.
Often triggers institutional selling.

**Strength Percentage:**
- Positive % = Golden Cross (bullish)
  - 0-2% = Weak, just crossed, may reverse
  - 2-5% = Moderate strength
  - 5%+ = Strong uptrend, well-established

- Negative % = Death Cross (bearish)
  - 0 to -2% = Weak, just crossed
  - -2 to -5% = Moderate weakness
  - -5%+ = Strong downtrend

**How to use:**
- Golden Cross + High strength % = Strong BUY (ride the uptrend)
- Death Cross + Large negative % = Avoid or SELL
- Recent Golden Cross (strength 1-3%) = Early entry opportunity
- Golden Cross losing strength = Uptrend weakening, be cautious
        """,
        "sql": """
            SELECT 
                s.symbol,
                s.company_name,
                ROUND(dp.close_price, 2) AS price,
                ROUND(mi.sma_50, 2) AS ma_50,
                ROUND(mi.sma_200, 2) AS ma_200,
                ROUND((mi.sma_50 - mi.sma_200) / mi.sma_200 * 100, 2) AS strength_pct,
                CASE 
                    WHEN mi.sma_50 > mi.sma_200 THEN 'Golden Cross - Bullish'
                    ELSE 'Death Cross - Bearish'
                END AS trend_signal
            FROM momentum_indicators mi
            JOIN stocks s ON mi.stock_id = s.stock_id
            JOIN daily_prices dp ON mi.stock_id = dp.stock_id 
                AND mi.trade_date = dp.trade_date
            WHERE dp.trade_date = (SELECT MAX(trade_date) FROM daily_prices)
            ORDER BY strength_pct DESC
        """
        },
        
        "Stock Performance History": {
        "description": "Year-by-year returns for all stocks (2011-2025)",
        "explanation": """
**What this query shows:**
Annual returns for each stock across the entire 15-year dataset.

**Columns explained:**
- symbol: Stock ticker
- year: Calendar year
- start_price: Price on Jan 1st
- end_price: Price on Dec 31st
- annual_return: Percentage gain/loss for that year
- days_traded: Number of trading days that year

**How to use:**
See which stocks performed best in which years.
Identify consistent performers vs volatile stocks.
Understand historical patterns.

**Example insights:**
- TCS gained 45% in 2017 but lost 12% in 2018
- Reliance was negative in 2020 crash but recovered 35% in 2021
- HDFC Bank has been positive 11 out of 15 years

This demonstrates SQL aggregation across the ENTIRE 15-year dataset.
        """,
        "sql": """
            WITH yearly_data AS (
                SELECT 
                    s.symbol,
                    YEAR(dp.trade_date) AS year,
                    MIN(dp.trade_date) AS year_start,
                    MAX(dp.trade_date) AS year_end,
                    COUNT(*) AS days_traded
                FROM daily_prices dp
                JOIN stocks s ON dp.stock_id = s.stock_id
                WHERE YEAR(dp.trade_date) >= 2011
                GROUP BY s.symbol, YEAR(dp.trade_date)
            ),
            prices AS (
                SELECT 
                    s.symbol,
                    YEAR(dp.trade_date) AS year,
                    dp.close_price,
                    dp.trade_date,
                    ROW_NUMBER() OVER (PARTITION BY s.stock_id, YEAR(dp.trade_date) 
                                      ORDER BY dp.trade_date ASC) AS rn_start,
                    ROW_NUMBER() OVER (PARTITION BY s.stock_id, YEAR(dp.trade_date) 
                                      ORDER BY dp.trade_date DESC) AS rn_end
                FROM daily_prices dp
                JOIN stocks s ON dp.stock_id = s.stock_id
                WHERE YEAR(dp.trade_date) >= 2011
            )
            SELECT 
                yd.symbol,
                yd.year,
                ROUND(p_start.close_price, 2) AS start_price,
                ROUND(p_end.close_price, 2) AS end_price,
                ROUND((p_end.close_price - p_start.close_price) / p_start.close_price * 100, 2) AS annual_return,
                yd.days_traded
            FROM yearly_data yd
            JOIN prices p_start ON yd.symbol = p_start.symbol 
                AND yd.year = p_start.year 
                AND p_start.rn_start = 1
            JOIN prices p_end ON yd.symbol = p_end.symbol 
                AND yd.year = p_end.year 
                AND p_end.rn_end = 1
            ORDER BY yd.symbol, yd.year DESC
            LIMIT 150
        """
    },
    
    "RSI Patterns Analysis": {
        "description": "How often each stock becomes oversold/overbought (15 years)",
        "explanation": """
**What this query shows:**
Total count of oversold and overbought signals across 15 years for each stock.

**Columns explained:**
- symbol: Stock ticker
- total_days: Total trading days in database
- oversold_count: Days where RSI < 30
- overbought_count: Days where RSI > 70
- oversold_pct: What % of time stock was oversold
- overbought_pct: What % of time stock was overbought

**Insights:**

**High oversold %:**
Stock frequently drops sharply (volatile, opportunity-rich)
Good for momentum traders who buy dips

**High overbought %:**
Stock frequently rallies (strong momentum stock)
May indicate consistent uptrend

**Low both %:**
Stock trades in tight range (stable, boring)
Fewer trading opportunities

**Example interpretation:**
- ITC: 8% oversold, 12% overbought = Volatile, good for trading
- TCS: 2% oversold, 3% overbought = Stable, fewer signals
- Reliance: 15% oversold = Frequently oversold, buying opportunities

This analyzes RSI behavior across the ENTIRE 15-year dataset.
Over 37,000 daily RSI readings analyzed.
        """,
        "sql": """
            SELECT 
                s.symbol,
                COUNT(*) AS total_days,
                SUM(CASE WHEN mi.rsi_14 < 30 THEN 1 ELSE 0 END) AS oversold_count,
                SUM(CASE WHEN mi.rsi_14 > 70 THEN 1 ELSE 0 END) AS overbought_count,
                ROUND(SUM(CASE WHEN mi.rsi_14 < 30 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS oversold_pct,
                ROUND(SUM(CASE WHEN mi.rsi_14 > 70 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS overbought_pct,
                ROUND(AVG(mi.rsi_14), 2) AS avg_rsi
            FROM momentum_indicators mi
            JOIN stocks s ON mi.stock_id = s.stock_id
            WHERE mi.rsi_14 IS NOT NULL
            GROUP BY s.symbol
            ORDER BY oversold_pct DESC
        """
    
    },
}

MEAN_REVERSION_QUERIES = {
    "All Pairs Status": {
        "description": "Current Z-scores for all 5 pairs",
        "explanation": """
**What this query shows:**
Latest Z-score for each of the 5 stock pairs we track.

**Columns explained:**
- pair_name: Which two stocks (e.g., HDFCBANK-ICICIBANK)
- stock_a / stock_b: The two individual stocks in the pair
- current_ratio: Today's price ratio (Price of A / Price of B)
- z_score: Statistical divergence measure
- trade_signal: What action to take

**Understanding Z-Scores:**

Z-score measures how far the current price ratio has diverged from its historical average.

- Z = 0: Pair trading at normal ratio (no opportunity)
- Z = +1 or -1: Slight divergence (watch, not actionable yet)
- Z > +2: Stock A is expensive relative to Stock B (TRADE SIGNAL)
- Z < -2: Stock A is cheap relative to Stock B (TRADE SIGNAL)
- Z > +3 or < -3: Extreme divergence (very high confidence trade)

**Trade Signals Explained:**

**SHORT A / LONG B** (when Z > 2):
- Stock A became expensive compared to Stock B
- Short (sell) ₹1,00,000 of Stock A
- Long (buy) ₹1,00,000 of Stock B
- Profit when ratio normalizes

**LONG A / SHORT B** (when Z < -2):
- Stock A became cheap compared to Stock B
- Long (buy) ₹1,00,000 of Stock A
- Short (sell) ₹1,00,000 of Stock B
- Profit when ratio normalizes

**WATCH** (when 1 < |Z| < 2):
- Moderate divergence
- Monitor closely, prepare to trade if it crosses ±2

**NEUTRAL** (when |Z| < 1):
- Normal relationship
- No trading opportunity

**How to use:**
1. Scan the trade_signal column for "SHORT A / LONG B" or "LONG A / SHORT B"
2. These are actionable trades you can execute today
3. Equal amounts in both legs (₹1L short A + ₹1L long B)
4. Exit when Z-score returns to -0.5 to +0.5 range
5. Typical holding period: 5-7 trading days

**Why pairs trading:**
You profit from the ratio change, not market direction.
If market crashes, both stocks fall, but the ratio still converges.
Market-neutral strategy with lower risk than directional trades.
        """,
        "sql": """
            SELECT 
                sp.pair_name,
                a1.symbol AS stock_a,
                a2.symbol AS stock_b,
                ROUND(pm.price_ratio, 4) AS current_ratio,
                ROUND(pm.z_score, 2) AS z_score,
                CASE 
                    WHEN pm.z_score > 2 THEN 'SHORT A / LONG B'
                    WHEN pm.z_score < -2 THEN 'LONG A / SHORT B'
                    WHEN ABS(pm.z_score) > 1 THEN 'WATCH'
                    ELSE 'NEUTRAL'
                END AS trade_signal
            FROM pair_metrics pm
            JOIN stock_pairs sp ON pm.pair_id = sp.pair_id
            JOIN stocks a1 ON sp.stock_a_id = a1.stock_id
            JOIN stocks a2 ON sp.stock_b_id = a2.stock_id
            WHERE pm.trade_date = (
                SELECT MAX(trade_date) FROM pair_metrics WHERE pair_id = pm.pair_id
            )
            ORDER BY ABS(pm.z_score) DESC
        """
    },
    
    "Pairs Diverged High (Z > 2)": {
        "description": "Stock A expensive vs B - Short A, Long B",
        "explanation": """
**What this query shows:**
All instances in the last 30 days where a pair's Z-score exceeded +2.

**Columns explained:**
- pair_name: Which stock pair
- stock_a / stock_b: The two stocks
- trade_date: When this divergence occurred
- current_ratio: Price ratio on that day (A/B)
- normal_ratio: 20-day average ratio
- z_score: Statistical divergence (higher = more extreme)

**What Z > 2 means:**
Stock A has become expensive relative to Stock B by more than 2 standard deviations.
This is statistically unusual and likely to revert to normal.

**The Trade Setup:**

When you see Z > 2 for a pair:

1. Short ₹1,00,000 worth of Stock A
   - You're betting Stock A will fall (or rise slower)

2. Long ₹1,00,000 worth of Stock B
   - You're betting Stock B will rise (or fall slower)

3. Exit when Z-score returns to -0.5 to +0.5
   - The ratio has normalized back to historical average

4. Expected holding period: 5-7 trading days
   - Most mean reversions complete within a week

**Example:**

HDFCBANK-ICICIBANK shows Z = 2.5 on April 15th
- Current ratio: 1.05 (HDFC costs ₹1.05 for every ₹1 of ICICI)
- Normal ratio: 0.98 (historically trades near parity)
- HDFC is expensive vs ICICI

**Execute trade:**
- Short ₹1,00,000 HDFCBANK (sell 60 shares at ₹1,650)
- Long ₹1,00,000 ICICIBANK (buy 85 shares at ₹1,175)
- Total capital: ₹2,00,000 (₹1L each side)

**Outcome scenarios:**

Scenario A: HDFC falls 2%, ICICI rises 1%
- Profit from HDFC short: ₹2,000
- Profit from ICICI long: ₹1,000
- Total profit: ₹3,000 (1.5% return)

Scenario B: Both fall 5% but ratio normalizes
- Loss from ICICI long: ₹5,000
- Profit from HDFC short: ₹5,000 + convergence profit
- Still profitable because ratio converged

**How to use this table:**
1. Look for recent divergences (3-7 days ago)
2. Check if Z-score is still above 2 in "All Pairs Status"
3. If yes, execute the trade today
4. If Z already normalized, opportunity has passed

**Risk:**
Pair correlation may have broken down (fundamental change in business).
Always check if both companies are in same sector and still comparable.
        """,
        "sql": """
            SELECT 
                sp.pair_name,
                a1.symbol AS stock_a,
                a2.symbol AS stock_b,
                pm.trade_date,
                ROUND(pm.price_ratio, 4) AS current_ratio,
                ROUND(pm.ratio_sma_20, 4) AS normal_ratio,
                ROUND(pm.z_score, 2) AS z_score
            FROM pair_metrics pm
            JOIN stock_pairs sp ON pm.pair_id = sp.pair_id
            JOIN stocks a1 ON sp.stock_a_id = a1.stock_id
            JOIN stocks a2 ON sp.stock_b_id = a2.stock_id
            WHERE pm.z_score > 2
                AND pm.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ORDER BY pm.trade_date DESC, pm.z_score DESC
            LIMIT 20
        """
    },
    
    "Pairs Diverged Low (Z < -2)": {
        "description": "Stock A cheap vs B - Long A, Short B",
        "explanation": """
**What this query shows:**
All instances in the last 30 days where a pair's Z-score fell below -2.

**Columns explained:**
- pair_name: Which stock pair
- stock_a / stock_b: The two stocks
- trade_date: When this divergence occurred
- current_ratio: Price ratio on that day (A/B)
- normal_ratio: 20-day average ratio
- z_score: Statistical divergence (more negative = more extreme)

**What Z < -2 means:**
Stock A has become cheap relative to Stock B by more than 2 standard deviations.
This is the opposite situation of Z > 2.

**The Trade Setup:**

When you see Z < -2 for a pair:

1. Long ₹1,00,000 worth of Stock A
   - You're betting Stock A will rise (or fall slower)

2. Short ₹1,00,000 worth of Stock B
   - You're betting Stock B will fall (or rise slower)

3. Exit when Z-score returns to -0.5 to +0.5
   - The ratio has normalized back to historical average

4. Expected holding period: 5-7 trading days

**Example:**

TCS-INFY shows Z = -2.3 on April 10th
- Current ratio: 0.92 (TCS costs ₹0.92 for every ₹1 of INFY)
- Normal ratio: 1.00 (historically trades near parity)
- TCS is cheap vs INFY

**Execute trade:**
- Long ₹1,00,000 TCS (buy 28 shares at ₹3,550)
- Short ₹1,00,000 INFY (sell 69 shares at ₹1,450)
- Total capital: ₹2,00,000

**Outcome:**
As ratio normalizes, TCS outperforms INFY.
You profit from both the TCS rise and INFY relative weakness.

**How to use this table:**
1. Recent divergences are most actionable (last 5 days)
2. Cross-check with "All Pairs Status" - is Z still < -2?
3. If yes, execute the trade
4. If Z already recovered, you missed the window

**Risk Management:**
- Never trade if fundamental story changed (merger, regulatory issue, etc.)
- Both stocks should still be in same sector
- Historical correlation should be > 0.7
        """,
        "sql": """
            SELECT 
                sp.pair_name,
                a1.symbol AS stock_a,
                a2.symbol AS stock_b,
                pm.trade_date,
                ROUND(pm.price_ratio, 4) AS current_ratio,
                ROUND(pm.ratio_sma_20, 4) AS normal_ratio,
                ROUND(pm.z_score, 2) AS z_score
            FROM pair_metrics pm
            JOIN stock_pairs sp ON pm.pair_id = sp.pair_id
            JOIN stocks a1 ON sp.stock_a_id = a1.stock_id
            JOIN stocks a2 ON sp.stock_b_id = a2.stock_id
            WHERE pm.z_score < -2
                AND pm.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ORDER BY pm.trade_date DESC, pm.z_score ASC
            LIMIT 20
        """
    },
    
    "Pair History (30 Days)": {
        "description": "Z-score movement for HDFC-ICICI pair over last 30 days",
        "explanation": """
**What this query shows:**
Daily Z-scores for the HDFC-ICICI pair over the last 30 days.

**Columns explained:**
- pair_name: HDFCBANK-ICICIBANK
- trade_date: Each trading day
- ratio: Daily price ratio (HDFC price / ICICI price)
- z_score: Statistical divergence that day
- trade_signal: What signal was generated that day

**How to read this:**

You can see the Z-score trend over time:
- Is it moving toward +2 or -2 (diverging)?
- Did it cross ±2 recently (trade signal)?
- Has it already reverted back to 0 (opportunity passed)?

**Pattern Recognition:**

**Trending Divergence:**
If Z-score is steadily increasing from 1.0 → 1.5 → 1.8 → 2.1
→ Divergence is building, trade signal just triggered

**Quick Reversion:**
If Z-score went 2.5 → 2.0 → 1.0 → 0.5 in 3 days
→ Fast mean reversion, profitable trade completed

**Persistent Divergence:**
If Z-score stays above 2 for 10+ days
→ Either strong trend or correlation breakdown
→ Check fundamentals before trading

**How to use:**
1. See if recent signals (Z > 2 or Z < -2) were profitable
2. Check current trend direction
3. Identify if pair is approaching actionable levels
4. Understand typical reversion timeframes for this pair

**Why this matters:**
Different pairs have different behaviors:
- Banking pairs (HDFC-ICICI) revert quickly (3-5 days)
- IT pairs (TCS-INFY) may take longer (7-10 days)
- Cross-sector pairs are less reliable

Historical behavior helps set realistic exit expectations.
        """,
        "sql": """
            SELECT 
                sp.pair_name,
                pm.trade_date,
                ROUND(pm.price_ratio, 4) AS ratio,
                ROUND(pm.z_score, 2) AS z_score,
                CASE 
                    WHEN pm.z_score > 2 THEN 'SHORT A / LONG B'
                    WHEN pm.z_score < -2 THEN 'LONG A / SHORT B'
                    ELSE 'NO TRADE'
                END AS trade_signal
            FROM pair_metrics pm
            JOIN stock_pairs sp ON pm.pair_id = sp.pair_id
            WHERE pm.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                AND sp.pair_name = 'HDFCBANK-ICICIBANK'
            ORDER BY pm.trade_date DESC
        """
    }
}

QUERY_OPTIONS = {
    "MOMENTUM": MOMENTUM_QUERIES,
    "MEAN_REVERSION": MEAN_REVERSION_QUERIES
}

st.title("Quantitative Stock Screener")
st.caption("DBMS Project | Momentum + Mean Reversion Strategies | NSE India")

with st.sidebar:
    st.header("Analysis Options")
    
    strategy = st.radio("Select Strategy:", ["MOMENTUM", "MEAN_REVERSION"])
    query_name = st.selectbox("Choose Query:", list(QUERY_OPTIONS[strategy].keys()))
    st.info(QUERY_OPTIONS[strategy][query_name]["description"])
    
    run_query = st.button("Run Analysis", type="primary", use_container_width=True)
    
    st.markdown("---")
    st.subheader("Database Info")
    
    try:
        stats_df = get_database_stats()
        
        if 'database_stats' not in st.session_state:
            st.session_state.database_stats = stats_df
        
        st.metric("Stocks", stats_df['stocks'].iloc[0])
        st.metric("Trading Days", stats_df['days'].iloc[0])
        st.caption(f"Data: {stats_df['from_date'].iloc[0]} to {stats_df['to_date'].iloc[0]}")
    except:
        st.error("Database connection failed")
    
    st.markdown("---")
    st.subheader("Data Update")
    
    if st.button("Refresh Latest Data", use_container_width=True):
        old_stats = st.session_state.get('database_stats')
        old_days = old_stats['days'].iloc[0] if old_stats is not None else 0
        
        with st.spinner("Updating from Yahoo Finance..."):
            result = update_latest_data()
        
        if result.get('already_updated'):
            st.info("Database already has latest data (market may be closed)")
        elif result['success']:
            new_stats = get_database_stats()
            new_days = new_stats['days'].iloc[0]
            days_added = new_days - old_days
            
            st.session_state.database_stats = new_stats
            
            st.success(f"Added {result['new_trading_days']} new trading day(s)")
            st.info(f"Total trading days: {old_days} → {new_days}")
            
            if result['errors']:
                st.warning(f"{len(result['errors'])} errors occurred")
                with st.expander("View errors"):
                    for e in result['errors']:
                        st.text(e)
        else:
            st.error("No new data available (market closed or weekend)")
            if result['errors']:
                with st.expander("View errors"):
                    for e in result['errors']:
                        st.text(e)
    
    st.caption("Fetches last 7 days from Yahoo Finance")

if run_query:
    st.subheader(f"Results: {query_name}")
    
    try:
        with st.spinner("Running query..."):
            results_df = execute_query(QUERY_OPTIONS[strategy][query_name]["sql"])
        
        if len(results_df) > 0:
            st.dataframe(results_df, use_container_width=True, hide_index=True)
            
            with st.expander("What does this mean? How do I use this?", expanded=True):
                st.markdown(QUERY_OPTIONS[strategy][query_name]["explanation"])
            
            csv = results_df.to_csv(index=False)
            st.download_button(
                "Download CSV",
                csv,
                f"{query_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                "text/csv"
            )
        else:
            st.info("No results found for this query in the last 30 days")
            with st.expander("About this query", expanded=True):
                st.markdown(QUERY_OPTIONS[strategy][query_name]["explanation"])
    
    except Exception as e:
        st.error(f"Query error: {e}")

else:
    st.info("Select a strategy and query from sidebar, then click 'Run Analysis'")
    
    st.markdown("""
    ## Two Trading Strategies
    
    ### MOMENTUM (Trend Following)
    Identify stocks with strong trends using technical indicators.
    
    **What it does:** Finds stocks in strong uptrends (buy) or downtrends (sell) using moving averages and RSI.
 
    **Current Market Queries (Last 30 Days):**
    - **Market Overview** - All 10 stocks with BUY/SELL/HOLD signals
    - **Oversold Stocks** - RSI < 30 (potential bounce)
    - **Overbought Stocks** - RSI > 70 (take profit)
    - **Long Term Trends** - Golden Cross vs Death Cross
    
    **Historical Analysis Queries (15-Year Dataset):**
    - **Stock Performance History** - Year-by-year returns 2011-2025
    - **RSI Patterns Analysis** - Oversold/overbought frequency across 15 years
    
    **Best for:** Riding established trends, identifying entry/exit points
    
    ---
    
    ### MEAN REVERSION (Pairs Trading)
    Trade temporary divergences between correlated stock pairs.
    
    **What it does:** Finds when two similar stocks diverge unusually, then profit when they converge back.
    
    **Available Queries:**
    - **All Pairs Status** - Current Z-scores for all 5 pairs with trade signals
    - **Pairs Diverged High** - Stock A expensive vs B → Short A, Long B
    - **Pairs Diverged Low** - Stock A cheap vs B → Long A, Short B
    - **Pair History** - See how HDFC-ICICI pair behaved over last 30 days
    
    **Our 5 Tracked Pairs:**
    - HDFCBANK-ICICIBANK (Large private banks)
    - ICICIBANK-KOTAKBANK (Mid-tier private banks)
    - TCS-INFY (IT services giants)
    - HINDUNILVR-ITC (FMCG sector leaders)
    - HDFCBANK-SBIN (Private vs PSU bank)
    
    **Best for:** Market-neutral strategies, lower risk than directional trading
    
    ---
    
    ## How This Works
    
    **Database:** 37,000+ price records across 10 NSE stocks, 15 years of history (2011-2025)
    
    **Calculations:** All indicators (moving averages, RSI, Z-scores) calculated in SQL using window functions and stored procedures
    
    **Data Source:** Yahoo Finance API via Python ETL pipeline
    
    **Update Mechanism:** Click "Refresh Latest Data" to fetch last 7 trading days
    """)

st.markdown("---")
st.caption("Stock Screener | DBMS Project 2026 | Yahoo Finance | NSE India")