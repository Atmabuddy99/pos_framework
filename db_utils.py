import sqlite3
from datetime import datetime, date
import pandas as pd

class TradeDB:
    def __init__(self, db_path="trades.db"):
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        """Create necessary tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create trades table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            expiry DATE,
            strike REAL,
            entry_time TIMESTAMP,
            entry_price REAL,
            exit_time TIMESTAMP,
            exit_price REAL,
            quantity INTEGER,
            pnl REAL,
            pnl_pct REAL,
            status TEXT,
            exit_reason TEXT
        )
        """)
        
        conn.commit()
        conn.close()
    
    def save_trade(self, trade):
        """Save a trade from TradeBook"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Parse trade data
            symbol = trade['symbol']
            expiry = trade.get('expiry')
            strike = trade.get('strike')
            timestamp = trade['ts']
            price = trade['price']
            quantity = trade['qty']
            order = trade['order']

            # Insert trade
            cursor.execute("""
            INSERT INTO trades (
                symbol, expiry, strike, entry_time, entry_price, quantity, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                expiry,
                strike,
                timestamp,
                price,
                quantity,
                'OPEN' if order == 'S' else 'CLOSED'
            ))

            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"Error saving trade: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()

    def get_trades(self, symbol=None, status=None):
        """Get trades from database with optional filters"""
        conn = sqlite3.connect(self.db_path)
        
        query = "SELECT * FROM trades"
        params = []
        
        if symbol or status:
            query += " WHERE"
            conditions = []
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)
            if status:
                conditions.append("status = ?")
                params.append(status)
            query += " AND ".join(conditions)
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    def update_trade(self, trade_id, exit_price, exit_time, pnl=None):
        """Update a trade with exit information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
            UPDATE trades 
            SET exit_time = ?, exit_price = ?, pnl = ?, status = 'CLOSED'
            WHERE trade_id = ?
            """, (exit_time, exit_price, pnl, trade_id))
            
            conn.commit()
        except Exception as e:
            print(f"Error updating trade: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_open_trades(self):
        """Get all open trades"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM trades WHERE status = 'OPEN'", conn)
        conn.close()
        return df
    
    def get_trades_history(self, start_date=None, end_date=None):
        """Get trade history with optional date filtering"""
        conn = sqlite3.connect(self.db_path)
        query = "SELECT * FROM trades WHERE status = 'CLOSED'"
        
        if start_date:
            query += f" AND entry_time >= '{start_date}'"
        if end_date:
            query += f" AND entry_time <= '{end_date}'"
            
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
