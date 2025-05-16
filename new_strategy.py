import pandas as pd
from datetime import datetime, date
from typing import Dict, Tuple
from genc import GenericStrategy
import logging
import time

class OutSellStrategy(GenericStrategy):
    def __init__(self, data_dir: str, expiry_list):
        # Initialize parent class first
        super().__init__(data_dir, expiry_list)
        
        # Set our own attributes
        self.exit_signal = False
        self.entry_price = None
        self.selected_strike = None
        self.position_expiry = None
        self.entry_date = None
        self.position_details = None
        self.exp_to_trade=None
        self.current_time=None
        self.spot=0
        self.initial_note_price=None
        self.roll=None
        #self.expiries_to_trade = None
        
    def get_atm_strike(self, data: pd.DataFrame, index: int =0) -> float:
        """
        Get ATM strike using put_position column
        ATM is where put_position is 0
        """
        atm_data = data[data['put_position'] == index]
        if not atm_data.empty:
            return atm_data['strike'].iloc[0]
        return None
    
    def entry(self, data: pd.DataFrame, timestamp: str) -> Dict:
        """
        Entry logic for put selling strategy:
        - Enter at market open (9:15)
        - Sell ATM put option
        Returns: Dictionary with entry parameters if entry conditions met, None otherwise
        """
      #  if self.last_trade_updated_time:
       #     a=1
       # else:
        #    if timestamp != "09:15:00":
         #       return None
            
        atm_strike = self.get_atm_strike(data, 1)
        if atm_strike is None:
            logging.warning(f"Could not find ATM strike at {timestamp}")
            return None  
        self.selected_strike = atm_strike
        self.position_expiry = self.current_expiry
        self.entry_date = self.current_date
        
        put_data = data[data['strike'] == atm_strike]  # Only filter by strike
        
        if put_data.empty:
            return None
            
        self.entry_price = put_data['put_close'].iloc[0] 
         # Using put_close as the entry price
        self.entry_price_ce=put_data["call_close"].iloc[0]

        straddle=self.entry_price+self.entry_price_ce
        hedge=int(50*round(straddle/50))

        self.roll=hedge
        ce_hedge=atm_strike+hedge
        pe_hedge=atm_strike-hedge

        ce_hprice=data[data["strike"]==ce_hedge]["call_close"].iloc[0]
        pe_hprice=data[data["strike"]==pe_hedge]["put_close"].iloc[0]

        self.initial_note_price=self.spot


        print(self.spot,straddle,ce_hedge,pe_hedge,ce_hprice,pe_hprice,self.roll,self.entry_price_ce,self.entry_price)
       
        
        #print(self.entry_price,self.entry_price_ce)

        #self.roll=50
       
        
        # Save full position details
        self.position_details = {
            "strike": atm_strike,
            "option_type": "PE",
            "quantity": 1,  
            "entry_price": self.entry_price,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta": put_data['put_delta'].iloc[0],
            "entry_iv": put_data['put_iv'].iloc[0]
        }
        self.position_details1 = {
            "strike": atm_strike,
            "option_type": "CE",
            "quantity": 1,  
            "entry_price": self.entry_price_ce,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta": put_data['call_delta'].iloc[0],
            "entry_iv": put_data['call_iv'].iloc[0]
        }
        self.position_details2 = {
            "strike": pe_hedge,
            "option_type": "PE",
            "quantity": 1,  
            "entry_price":pe_hprice,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta": 0,
            "entry_iv": 0
        }
        self.position_details3 = {
            "strike": ce_hedge,
            "option_type": "CE",
            "quantity": 1,  
            "entry_price": ce_hprice,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta":0,
            "entry_iv": 0
        }

        if self.position_details:
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details['strike']}|{self.position_details['option_type']}",
            expiry=self.position_details['expiry'],
            strike=self.position_details['strike'],
            entry_price=self.position_details['entry_price'],
            quantity=self.position_details['quantity'],
            order="sell"
                        )
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details1['strike']}|{self.position_details1['option_type']}",
            expiry=self.position_details1['expiry'],
            strike=self.position_details1['strike'],
            entry_price=self.position_details1['entry_price'],
            quantity=self.position_details1['quantity'],
            order="sell",
                        )
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details2['strike']}|{self.position_details2['option_type']}",
            expiry=self.position_details2['expiry'],
            strike=self.position_details2['strike'],
            entry_price=self.position_details2['entry_price'],
            quantity=self.position_details2['quantity'],
            order="buy"
                        )
            
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details3['strike']}|{self.position_details3['option_type']}",
            expiry=self.position_details3['expiry'],
            strike=self.position_details3['strike'],
            entry_price=self.position_details3['entry_price'],
            quantity=self.position_details3['quantity'],
            order="buy"
                        )


       
       
        
        logging.info(f" {self.current_date} : {self.current_time}:Entry signal: Selling ATM PUT at strike {atm_strike}, current date is {self.current_date} "
             f"price {self.entry_price}, expiry {self.current_expiry}, "
             f"delta {put_data['put_delta'].iloc[0]:.2f}, IV {put_data['put_iv'].iloc[0]:.2f}"),

        
        return self.position_details
    
    def enter_more(self, data: pd.DataFrame, timestamp: str) -> Dict:
      

        print("enterning more baby")
        print("debugging starts here ")
        print(self.spot,self.current_date,self.current_expiry)
        print("debuggin ends here")
        
        self.initial_note_price=self.spot
            
        atm_strike = self.get_atm_strike(data, 1)
        if atm_strike is None:
            logging.warning(f"Could not find ATM strike at {timestamp}")
            return None
            
        self.selected_strike = atm_strike
        self.position_expiry = self.current_expiry
        self.entry_date = self.current_date
        
        put_data = data[data['strike'] == atm_strike]  # Only filter by strike
        
        if put_data.empty:
            return None
            
        self.entry_price = put_data['put_close'].iloc[0] 
         # Using put_close as the entry price
        self.entry_price_ce=put_data["call_close"].iloc[0]

        straddle=self.entry_price+self.entry_price_ce
        hedge=int(50*round(straddle/50))
        ce_hedge=atm_strike+hedge
        pe_hedge=atm_strike-hedge

        self.roll=hedge

        ce_hprice=data[data["strike"]==ce_hedge]["call_close"].iloc[0]
        pe_hprice=data[data["strike"]==pe_hedge]["put_close"].iloc[0]


        print(self.spot,straddle,ce_hedge,pe_hedge,ce_hprice,pe_hprice)
        print(self.entry_price,self.entry_price_ce)
        self.position_details = {
            "strike": atm_strike,
            "option_type": "PE",
            "quantity": 1,  
            "entry_price": self.entry_price,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta": put_data['put_delta'].iloc[0],
            "entry_iv": put_data['put_iv'].iloc[0]
        }
        self.position_details1 = {
            "strike": atm_strike,
            "option_type": "CE",
            "quantity": 1,  
            "entry_price": self.entry_price_ce,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta": put_data['call_delta'].iloc[0],
            "entry_iv": put_data['call_iv'].iloc[0]
        }
        self.position_details2 = {
            "strike": pe_hedge,
            "option_type": "PE",
            "quantity": 1,  
            "entry_price":pe_hprice,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta": 0,
            "entry_iv": 0
        }
        self.position_details3 = {
            "strike": ce_hedge,
            "option_type": "CE",
            "quantity": 1,  
            "entry_price": ce_hprice,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta":0,
            "entry_iv": 0
        }

        if self.position_details:
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details['strike']}|{self.position_details['option_type']}",
            expiry=self.position_details['expiry'],
            strike=self.position_details['strike'],
            entry_price=self.position_details['entry_price'],
            quantity=self.position_details['quantity'],
            order="sell"
                        )
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details1['strike']}|{self.position_details1['option_type']}",
            expiry=self.position_details1['expiry'],
            strike=self.position_details1['strike'],
            entry_price=self.position_details1['entry_price'],
            quantity=self.position_details1['quantity'],
            order="sell"
                        )
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details2['strike']}|{self.position_details2['option_type']}",
            expiry=self.position_details2['expiry'],
            strike=self.position_details2['strike'],
            entry_price=self.position_details2['entry_price'],
            quantity=self.position_details2['quantity'],
            order="buy"
                        )
            
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details3['strike']}|{self.position_details3['option_type']}",
            expiry=self.position_details3['expiry'],
            strike=self.position_details3['strike'],
            entry_price=self.position_details3['entry_price'],
            quantity=self.position_details3['quantity'],
            order="buy"
                        )
            
       
        
        logging.info(f" {self.current_date} : {self.current_time}:Entry signal: Selling ATM PUT at strike {atm_strike}, current date is {self.current_date} "
             f"price {self.entry_price}, expiry {self.current_expiry}, "
             f"delta {put_data['put_delta'].iloc[0]:.2f}, IV {put_data['put_iv'].iloc[0]:.2f}"),

        
        return self.position_details
    

    def adjust(self, position: Dict, data: pd.DataFrame, timestamp: str) -> Dict:
        """
        Adjustment logic for the position
        Returns: Dictionary with adjustment parameters if needed, None otherwise
        """
        if not self.position_details:
            return None

        try:
            pnl = self.tb.mtm(prices= dict(zip(data["strike"], zip(data["call_close"], data["put_close"]))))
        except:
            pnl={}
        
        if pnl:
            sum_pnl=sum(pnl.values())
            if (self.spot>=self.initial_note_price+self.roll) or (self.spot<=self.initial_note_price-self.roll):
                self.enter_more(data,timestamp)
                self.initial_note_price=self.spot
           

           
            is_expiry_day = str(self.current_date) == str(self.position_expiry)
            if sum_pnl<-10000:
                if self.exit1(data,timestamp,exit_message=True):
                    print(self.exp_to_trade,"sl")
                    logging.WARN("sl is hit ")
            if sum_pnl>10000:
                if self.exit1(data,timestamp,exit_message=True):
                    print(self.exp_to_trade,"target")
                    logging.WARN("target is hit ")
                    return True  

            if is_expiry_day:
                if timestamp >= "12:00:00":   
                    if self.exit1(data,timestamp,exit_message=True):
                        print(self.exp_to_trade,"timext")
                        return True             
            return None

    def exit1(self, data: pd.DataFrame, timestamp: str,exit_message : bool =False) -> bool:
        #print("here for exit")
        #a*5
        current_data = data[
            (data['strike'] == self.selected_strike)
        ]

        if current_data.empty:
            return False
            
        current_price = current_data['put_close'].iloc[0]


        pnl = self.tb.mtm(prices= dict(zip(data["strike"], zip(data["call_close"], data["put_close"]))))
       
        sum_pnl=sum(pnl.values())
        days_held=0
        pnl_percentage=1
        delta_change=1.5
        delta=1
        vega=8
        theta=15
        iv_change=0
        tte=5
        iv=5
        if exit_message:
            for pos,qty in self.tb.positions.items():
                strike, option_type = pos.split("|")
                side="sell" if qty>0 else "buy"
                dbb=data[data["strike"]==int(strike)]
                #print(dbb)
                index_="call_close" if option_type=="CE" else "put_close"
                price=dbb[index_].iloc[0]
                #print(pos,strike,price)
                qty=qty*(-1) if side=="buy" else qty*(1)
                self.tb.add_trade(
                            timestamp=str(self.current_date) + self.current_time,
                            symbol=pos,
                            price=price,
                            qty=qty,
                            order=side,
                            expiry=self.position_expiry,
                            strike=strike,
                        )

            

            logging.info(
                f"Exit signal: {exit_message}\n"
                f"Position held for {days_held} days\n"
                f"P&L: {sum_pnl:.2f} ({pnl_percentage:.2f}%)\n"
                f"Greeks: Delta={delta:.2f} ({delta_change:+.2f}), "
                f"Theta={theta:.2f}, Vega={vega:.2f}\n"
                f"IV: {iv:.2f}% ({iv_change:+.2f}%)\n"
                f"Time to expiry: {tte:.2f} days"
            )
           

           
            self.position_details = None  # Clear position details
            self.exit_signal = True



       

            return True
            
        return False



    
        
   
    def run_strategy(self, current_data: pd.DataFrame,update_time: str = None) -> bool:
        """
        Run the strategy for the current data
        Returns: True if strategy has exited position, False if still holding or no position
        """
       # print(self.expiries_to_trade,'scvbvjvcv')

        
        
        if update_time:
            unique_minutes = sorted([m for m in current_data["minute"].unique() if m > update_time])
        else:
            unique_minutes = sorted(current_data["minute"].unique())

        for timestamp in unique_minutes: 
            self.current_time=timestamp
            current_data_at_time = current_data[current_data["minute"] == timestamp]
            
            
            self.spot=current_data_at_time["spot_price"].iloc[0]

         #   strike_data = dict(zip(current_data_at_time['strike'], [{'put_close': put, 'put_delta': delta, 'put_gamma': gamma, 'theta': theta, 'vega': vega} 
          #                            for put, delta, gamma, theta, vega in zip(current_data_at_time['put_close'], current_data_at_time['put_delta'], 
           #                           current_data_at_time['put_gamma'], current_data_at_time['put_theta'], current_data_at_time['put_vega'])]))

            
            if not self.tb.open_positions:
                if not self.exit_signal: 
                    self.entry(current_data_at_time, timestamp)# Only try to enter if we haven't just exited
            
            elif self.tb.open_positions:
                current_position = self.tb.positions
                if self.adjust(current_position,current_data_at_time,timestamp):
                    return True 
             

        # Return false if we're still holding position or didn't enter
        return False
