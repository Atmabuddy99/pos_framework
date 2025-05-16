import pandas as pd
from datetime import datetime, date
from typing import Dict, Tuple
from genc import GenericStrategy
import logging

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
        if timestamp != "09:15:00":
            return None
            
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
            
        self.entry_price = put_data['put_close'].iloc[0]  # Using put_close as the entry price
        
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

        if self.position_details:
            self.enter_position(
            timestamp=timestamp,
            symbol=f"{self.position_details['strike']}|{self.position_details['option_type']}",
            expiry=self.position_details['expiry'],
            strike=self.position_details['strike'],
            entry_price=self.position_details['entry_price'],
            quantity=self.position_details['quantity']
                        )


       
       
        
        logging.info(f" {self.current_date} : {self.current_time}:Entry signal: Selling ATM PUT at strike {atm_strike}, current date is {self.current_date} "
             f"price {self.entry_price}, expiry {self.current_expiry}, "
             f"delta {put_data['put_delta'].iloc[0]:.2f}, IV {put_data['put_iv'].iloc[0]:.2f}"),

        
        return self.position_details
    
    def exit(self, position: Dict, data: pd.DataFrame, timestamp: str) -> bool:
        """
        Exit logic:
        1. Exit at expiry end of day (3:30 PM)
        2. Exit if loss exceeds 30% of entry price
        3. Exit if profit reaches 50% of entry price
        Returns: True if exit conditions met, False otherwise
        """
        if not self.position_details:
            return False
       

        current_data = data[
            (data['strike'] == self.selected_strike)
        ]

        if current_data.empty:
            return False
            
        current_price = current_data['put_close'].iloc[0]
        
        # Calculate P&L
        pnl = self.tb.mtm(prices=dict(zip(data["strike"], data["put_close"])))

       
        
        
        # Get Greeks for logging
        delta = current_data['put_delta'].iloc[0]
        theta = current_data['put_theta'].iloc[0]
        vega = current_data['put_vega'].iloc[0]
        iv = current_data['put_iv'].iloc[0]
        tte = current_data['tte'].iloc[0]
        
        # Calculate changes from entry
        delta_change = delta - self.position_details['entry_delta']
        iv_change = iv - self.position_details['entry_iv']
        days_held = (self.current_date - self.entry_date).days
        
        # Check if it's expiry day
        is_expiry_day = str(self.current_date) == str(self.position_expiry)
        pnl_percentage=1.5  # Compare dates as strings
        
        # Exit conditions with detailed logging
        
        exit_message = None
        if is_expiry_day:
            if timestamp >= "15:15:00":
                exit_message = f"Expiry day end (Expiry: {self.position_expiry})"
            elif timestamp >= "15:25:00":  # Start squaring off 5 minutes before market close on expiry
                exit_message = f"Expiry day square off (Expiry: {self.position_expiry})"
       
        sum_pnl=sum(pnl.values())

        #print(type(exit_message),type(days_held),type(pnl),type(pnl_percentage))
        if exit_message:
            logging.info(
                f"Exit signal: {exit_message}\n"
                f"Position held for {days_held} days\n"
                f"P&L: {sum_pnl:.2f} ({pnl_percentage:.2f}%)\n"
                f"Greeks: Delta={delta:.2f} ({delta_change:+.2f}), "
                f"Theta={theta:.2f}, Vega={vega:.2f}\n"
                f"IV: {iv:.2f}% ({iv_change:+.2f}%)\n"
                f"Time to expiry: {tte:.2f} days"
            )
            # Exit the position
            self.enter_position(
                timestamp=timestamp,
                symbol=f"{self.selected_strike}|PE",
                expiry=self.position_expiry,
                strike=self.selected_strike,
                entry_price=current_price,
                quantity=position['quantity'],
                
            )

           
            self.position_details = None  # Clear position details
            self.exit_signal = True

            return True
            
        return False

    def adjust(self, position: Dict, data: pd.DataFrame, timestamp: str) -> Dict:
        """
        Adjustment logic for the position
        Returns: Dictionary with adjustment parameters if needed, None otherwise
        """
        if not self.position_details:
            return None

        try:
            pnl = self.tb.mtm(prices=dict(zip(data["strike"], data["put_close"])))
        except:
            pnl={}
        
        if pnl:
            sum_pnl=sum(pnl.values())

            #print(sum_pnl,timestamp)

           

            if sum_pnl<-100:
                if self.exit1(data,timestamp,exit_message=True):
                    print(self.expiries_to_trade,"sl")
                    return True
            if sum_pnl>100:
                if self.exit1(data,timestamp,exit_message=True):
                    print(self.expiries_to_trade,"target")
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


        pnl = self.tb.mtm(prices=dict(zip(data["strike"], data["put_close"])))
       
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

        #print(type(exit_message),type(days_held),type(pnl),type(pnl_percentage))
        if exit_message:
            logging.info(
                f"Exit signal: {exit_message}\n"
                f"Position held for {days_held} days\n"
                f"P&L: {sum_pnl:.2f} ({pnl_percentage:.2f}%)\n"
                f"Greeks: Delta={delta:.2f} ({delta_change:+.2f}), "
                f"Theta={theta:.2f}, Vega={vega:.2f}\n"
                f"IV: {iv:.2f}% ({iv_change:+.2f}%)\n"
                f"Time to expiry: {tte:.2f} days"
            )
            # Exit the position
            self.enter_position(
                timestamp=timestamp,
                symbol=f"{self.selected_strike}|PE",
                expiry=self.position_expiry,
                strike=self.selected_strike,
                entry_price=current_price,
                quantity=1,
                order="buy"
                
            )

           
            self.position_details = None  # Clear position details
            self.exit_signal = True

            return True
            
        return False



    
        
   
    def run_strategy(self, current_data: pd.DataFrame) -> bool:
        """
        Run the strategy for the current data
        Returns: True if strategy has exited position, False if still holding or no position
        """
        #print(current_data.columns)
        for timestamp in current_data["minute"].unique(): 
            self.current_time=timestamp
            #print(type(timestamp))
            
            current_data_at_time = current_data[current_data["minute"] == timestamp]

            strike_data = dict(zip(current_data_at_time['strike'], [{'put_close': put, 'put_delta': delta, 'put_gamma': gamma, 'theta': theta, 'vega': vega} 
                                      for put, delta, gamma, theta, vega in zip(current_data_at_time['put_close'], current_data_at_time['put_delta'], 
                                      current_data_at_time['put_gamma'], current_data_at_time['put_theta'], current_data_at_time['put_vega'])]))

            
            if not self.tb.open_positions:
                if not self.exit_signal: 
                    #print("coming for entry", timestamp)  # Only try to enter if we haven't just exited
                    self.entry(current_data_at_time, timestamp)
            
            elif self.tb.open_positions:
                #print(timestamp)
                current_position = self.tb.positions

                #print(current_position)

               

                #print(current_position)
               
                
                # Check for adjustments
                #adjust_params = self.adjust(current_position, current_data, timestamp)
                #if adjust_params and adjust_params['action'] == 'roll':
                 #   logging.info(
                  #      f"Rolling position: Delta {adjust_params['current_delta']:.2f} -> "
                   #     f"{adjust_params['new_delta']:.2f}, {adjust_params['days_to_expiry']:.1f} days to expiry"
                    #)
                    # Exit the current position before rolling
                #current_data_at_time = current_data[current_data["minute"] == timestamp]
                #self.exit_position(
                 #   timestamp=timestamp,
                  #  symbol=f"{self.selected_strike}PE",
                   # exit_price=current_data_at_time['put_close'].values[0],  # Use .values[0] to get scalar value
                   # reason=f"Rolling from delta {adjust_params['current_delta']:.2f}"
                #)
                #return True  # Return true since we exited the position
                
                # Check for exit
                #if self.exit(current_position, current_data_at_time, timestamp):
                #try:
                if self.adjust(current_position,current_data_at_time,timestamp):
                    return True 
                #except:
                 #   continue # Return true since we exited the position

        # Return false if we're still holding position or didn't enter
        return False
