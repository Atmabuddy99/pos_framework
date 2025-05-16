import pandas as pd
from datetime import datetime, date
from typing import Dict, Tuple
from genc import GenericStrategy
import logging

class OutSellStrategy(GenericStrategy):
    def __init__(self, data_dir: str, expiry_list: list):
        super().__init__(data_dir, expiry_list)
        self.exit_signal = False
        self.entry_price = None
        self.selected_strike = None
        self.position_expiry = None  # Track expiry of current position
        self.entry_date = None  # Track entry date
        self.position_details = None  # Track full position details
        
    def get_atm_strike(self, data: pd.DataFrame, timestamp: str) -> float:
        """
        Get ATM strike using put_position column
        ATM is where put_position is 0
        """
        atm_data = data[data['put_position'] == 0].loc[timestamp]
        if not atm_data.empty:
            return atm_data['strike']
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
            
        atm_strike = self.get_atm_strike(data, timestamp)
        if atm_strike is None:
            logging.warning(f"Could not find ATM strike at {timestamp}")
            return None
            
        self.selected_strike = atm_strike
        self.position_expiry = self.current_expiry
        self.entry_date = self.current_date
        
        # Get put option data for ATM strike
        put_data = data[
            (data['strike'] == atm_strike) & 
            (data['put_position'] == 0)  # Ensure it's ATM
        ].loc[timestamp]
        
        if put_data.empty:
            return None
            
        self.entry_price = put_data['put_close']  # Using put_close as the entry price
        
        # Save full position details
        self.position_details = {
            "strike": atm_strike,
            "option_type": "PE",
            "quantity": -1,  # Negative for selling
            "entry_price": self.entry_price,
            "expiry": self.current_expiry,
            "entry_date": self.current_date,
            "entry_time": timestamp,
            "entry_delta": put_data['put_delta'],
            "entry_iv": put_data['put_iv']
        }
        
        logging.info(f"Entry signal: Selling ATM PUT at strike {atm_strike}, "
                    f"price {self.entry_price}, expiry {self.current_expiry}, "
                    f"delta {put_data['put_delta']:.2f}, IV {put_data['put_iv']:.2f}")
        
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
            
        # Get current option data
        current_data = data[
            (data['strike'] == self.selected_strike)
        ].loc[timestamp]
        
        if current_data.empty:
            return False
            
        current_price = current_data['put_close']
        
        # Calculate P&L
        pnl = (self.entry_price - current_price) * abs(position['quantity'])
        pnl_percentage = (self.entry_price - current_price) / self.entry_price * 100
        
        # Get Greeks for logging
        delta = current_data['put_delta']
        theta = current_data['put_theta']
        vega = current_data['put_vega']
        iv = current_data['put_iv']
        tte = current_data['tte']
        
        # Calculate changes from entry
        delta_change = delta - self.position_details['entry_delta']
        iv_change = iv - self.position_details['entry_iv']
        days_held = (self.current_date - self.entry_date).days
        
        # Check if it's expiry day
        is_expiry_day = tte < 0.1  # Less than 0.1 days to expiry
        
        # Exit conditions with detailed logging
        exit_message = None
        if is_expiry_day and timestamp >= "15:30:00":
            exit_message = "Expiry day end"
        elif current_price >= self.entry_price * 1.3:  # 30% loss
            exit_message = "Stop loss hit"
        elif current_price <= self.entry_price * 0.5:  # 50% profit
            exit_message = "Target hit"
            
        if exit_message:
            logging.info(
                f"Exit signal: {exit_message}\n"
                f"Position held for {days_held} days\n"
                f"P&L: {pnl:.2f} ({pnl_percentage:.2f}%)\n"
                f"Greeks: Delta={delta:.2f} ({delta_change:+.2f}), "
                f"Theta={theta:.2f}, Vega={vega:.2f}\n"
                f"IV: {iv:.2f}% ({iv_change:+.2f}%)"
            )
            # Exit the position
            self.exit_position(
                timestamp=timestamp,
                symbol=f"{self.selected_strike}PE",
                exit_price=current_price,
                reason=exit_message
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
            
        current_data = data[data['strike'] == self.selected_strike].loc[timestamp]
        if current_data.empty:
            return None
            
        put_delta = current_data['put_delta']
        put_iv = current_data['put_iv']
        tte = current_data['tte']
        
        # Example adjustment logic
        if put_delta < -0.7 and tte > 1:  # Deep ITM and more than 1 day to expiry
            # Find next OTM strike
            otm_data = data[data['put_position'] == 1].loc[timestamp]  # First OTM strike
            if not otm_data.empty:
                new_strike = otm_data['strike']
                return {
                    "action": "roll",
                    "new_strike": new_strike,
                    "current_delta": put_delta,
                    "new_delta": otm_data['put_delta'],
                    "days_to_expiry": tte
                }
                
        return None

    def run_strategy(self, current_data: pd.DataFrame) -> bool:
        """
        Run the strategy for the current data
        Returns: True if strategy has exited position, False if still holding or no position
        """

        print(current_data.columns)
        for timestamp in current_data.index:
            # Check for entry if no position
            timestamp=current_data["minute"]

            if not self.tb.open_positions:
                if not self.exit_signal: 
                    print("coming for entry",timestamp) # Only try to enter if we haven't just exited
                    entry_params = self.entry(current_data, timestamp)
                    if entry_params:
                        self.enter_position(
                            timestamp=timestamp,
                            symbol=f"{entry_params['strike']}{entry_params['option_type']}",
                            expiry=entry_params['expiry'],
                            strike=entry_params['strike'],
                            entry_price=entry_params['entry_price'],
                            quantity=entry_params['quantity']
                        )
            
            # Check for adjustments and exits if has position
            elif self.tb.open_positions:
                current_position = self.tb.get_current_position()
                
                # Check for adjustments
                adjust_params = self.adjust(current_position, current_data, timestamp)
                if adjust_params and adjust_params['action'] == 'roll':
                    logging.info(
                        f"Rolling position: Delta {adjust_params['current_delta']:.2f} -> "
                        f"{adjust_params['new_delta']:.2f}, {adjust_params['days_to_expiry']:.1f} days to expiry"
                    )
                    # Exit the current position before rolling
                    current_data_at_time = current_data.loc[timestamp]
                    self.exit_position(
                        timestamp=timestamp,
                        symbol=f"{self.selected_strike}PE",
                        exit_price=current_data_at_time['put_close'],
                        reason=f"Rolling from delta {adjust_params['current_delta']:.2f}"
                    )
                    return True  # Return true since we exited the position
                
                # Check for exit
                if self.exit(current_position, current_data, timestamp):
                    return True  # Return true since we exited the position

        # Return false if we're still holding position or didn't enter
        return False
