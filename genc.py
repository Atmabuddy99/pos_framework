import os
import pandas as pd
from typing import List, Optional
import logging
from datetime import date, timedelta
import datetime
from tradebook import TradeBook
import json
import duckdb
import time


class GenericStrategy:
    def __init__(self, data_dir: str, expiry_list_file: str,is_intraday :bool=False):
        self.data_dir = data_dir
        self.current_date = None
        self.current_timestamp = None
        self.current_expiry = None
        self.options_data = None  # Cache for option data
        self.tb = TradeBook()  # Initialize tradebook
        self.expiry_cache = {}  # Cache for expiry dates
        self.multiple = True
        self.expiry_list_file = expiry_list_file
        self.expiries_to_trade = None
        self.all_tradebooks = []  # List to store all tradebooks
        self.is_intraday=is_intraday
        self.is_start_next_trade_next_day=True
        self.expiry_to_expiry=True

       
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('strategy.log'),
                logging.StreamHandler()
            ]
        )

    def get_all_expiries(self,path):
        """
        fetches unique expiries for the whole trading day 
        """
        sort_list=duckdb.query(f"SELECT DISTINCT expiry FROM '{path}'").to_df()['expiry'].tolist()
        self.expiries_to_trade = sorted(sort_list)
        
        

    def get_options_data(self,path,expiry:str):
        
        if isinstance(expiry,list):
            expiry=expiry[0]
        if expiry in self.expiry_cache:
            self.options_data = self.expiry_cache[expiry]
        else:
            self.options_data = duckdb.query(f"SELECT * FROM '{path}' WHERE expiry = '{expiry}'").to_df()

    def get_expiry(self,path,index=1,monthly=False):
        """
        Get particular necessary expiry
        """
        if monthly:
            return duckdb.query(f"SELECT DISTINCT expiry FROM '{path}' WHERE monthly_expiry_number = {index}").to_df().squeeze() if len(duckdb.query(f"SELECT DISTINCT expiry FROM '{path}' WHERE nearest_expiry = 1").to_df()) == 1 else None
        else:
            return  duckdb.query(f"SELECT DISTINCT expiry FROM '{path}' WHERE nearest_expiry = {index}").to_df().squeeze() if len(duckdb.query(f"SELECT DISTINCT expiry FROM '{path}' WHERE nearest_expiry = 1").to_df()) == 1 else None
        
    def enter_position(self, timestamp: str, symbol: str, expiry: str, strike: float, entry_price: float, quantity: int,order: str ="sell"):
        """
        Enter a new position
        """
        self.tb.add_trade(
            timestamp=str(self.current_date) + timestamp,
            symbol=symbol,
            price=entry_price,
            qty=quantity,
            order=order,
            expiry=expiry,
            strike=strike,
        )


    def _retract(self):
        """
        retract all open positions by removing trades
        """
        while self.tb.o > 0:
            for symbol in self.tb.open_positions:
                self.tb.remove_trade(symbol)

    

    def get_path(self):
        return os.path.join(self.data_dir, f'{self.current_date.strftime("%Y-%m-%d")}.parquet')
        

    

    def run(self, start_date: date, end_date: date, strategy_class):
        """
        Run the strategy between start_date and end_date
        Args:
            start_date: Starting date
            end_date: Ending date
            strategy_class: Strategy class to use
        """
        current_date = start_date
        strategy = None
        last_traded_time=None
        update_time=None
        path=None
        while current_date <= end_date:
            self.current_date = current_date
            print(self.current_date)
            # If we have an active strategy instance 
            if strategy and strategy.position_expiry and strategy.tb.positions:
                try:
                    strategy.last_trade_updated_time=None
                    logging.info(f"Strategy date path {path} ")
                    path=self.get_path()
                    self.get_options_data(path,strategy.position_expiry)
                    strategy.current_date = self.current_date
                    strategy.options_data = self.options_data  # Update with today's data
                    print(self.current_date,self.current_expiry,"derrrr")
                    
                    # Run strategy with current day's data
                    position_exited = strategy.run_strategy(self.options_data)

                    # If position was exited today or no positions in tradebook
                    if position_exited:
                        
                        last_traded_time=strategy.current_time
                        logging.info(f"Strategy exited position for expiry {strategy.position_expiry} @ {strategy.current_time}")
                        if strategy.tb.all_trades:  # Only append if there are trades
                            self.all_tradebooks.append(strategy.tb)
                            self.tb = TradeBook()
                            self.current_expiry = None
                        strategy = None
                        #current_date -= timedelta(days=1)
                   
                except (FileNotFoundError, duckdb.IOException) as e:
                    logging.warning(f"No data found for date {current_date} and expiry {strategy.position_expiry}")
                    if strategy:
                        if strategy.position_expiry==self.current_date:
                            if not(position_exited):
                                logging.info(f"there is no data on expiry {strategy.position_expiry}")
                                print(position_exited,self.current_date)
                                self._retract()
                                strategy=None
                                last_traded_time=None
          
            # If we don't have an active strategy or no open positions, look for new entry
            if strategy==None:
                if last_traded_time:
                    update_time=last_traded_time
                    last_traded_time=None
                    logging.info(f"strat only after {update_time}")
                else:
                    update_time=None

                path=self.get_path()
                logging.info(f"Strategy date path1 {path}")
                
                
                try:
                    self.get_all_expiries(path) 
                except:
                    current_date += timedelta(days=1)
                    continue
                
                
                print(self.expiries_to_trade,self.current_date)
                if not self.expiries_to_trade:
                    current_date += timedelta(days=1)
                    continue
                nearest_expiry = self.get_expiry(path,index=1)
                print(self.current_date,nearest_expiry,"expiry  gng to trade")
                print(type(self.current_date),type(nearest_expiry))

                
                near_d=datetime.date(int(nearest_expiry[:4]),int(nearest_expiry[5:7]),int(nearest_expiry[8:]))
                if self.current_date==near_d:
                    nearest_expiry1 = self.get_expiry(path,index=2)
                    logging.info(f"yes baby{nearest_expiry} {nearest_expiry1}nearest_expiry")
                    self.current_expiry = nearest_expiry1
                else:
                    self.current_expiry=nearest_expiry
                
                if self.current_expiry:
                    try:
                        self.get_options_data(path,nearest_expiry)
                    except  Exception as e :
                        logging.warning(f"No data found for date {current_date} and expiry {nearest_expiry}")
                        current_date += timedelta(days=1)
                        continue
                    # Create new strategy instance
                    strategy = strategy_class(self.data_dir, self.expiry_list_file)
                    strategy.current_date = self.current_date
                    strategy.current_expiry = self.current_expiry
                    strategy.options_data = self.options_data
                    strategy.exp_to_trade = self.expiries_to_trade 
                    # Run strategy to look for entry
                    position_exited = strategy.run_strategy(self.options_data,update_time=update_time)
                    update_time=None
                    
                    # If position was entered and exited on the same day
                    if position_exited:
                        if strategy.tb.all_trades:  # Only append if there are trades
                            self.all_tradebooks.append(strategy.tb)
                            self.tb = TradeBook()
                            self.current_expiry = None
                        strategy = None
        #              
                        # Reset for next entry
               




            print(current_date ,"before")
            current_date += timedelta(days=1)
            print(current_date ,"after")


        if strategy:
            if strategy and strategy.position_expiry and strategy.tb.positions:
                if not(position_exited):
                    logging.info(f"end date has been hit no more strategy running after this and there were some open positions {strategy.position_expiry}")
                    print(position_exited,self.current_date)
                    self._retract()
                    strategy=None
                    last_traded_time=None
        
