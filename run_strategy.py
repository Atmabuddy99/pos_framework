from strategy import OutSellStrategy
from datetime import date

# Directory containing your data files

data_dir = "e:/nifty_chain"  # Path to options chain data
expiry_list = "e:/my_coding/expiries_nifty"    # Adjust this to your data directory path

# List of expiry dates you want to trade#xpiry_list = ["2024-01-11 15:30:00", "2024-01-18 15:30:00"]  # Add your expiry dates

# Create strategy instance
strategy = OutSellStrategy(data_dir=data_dir, expiry_list=expiry_list)

# Set start and end dates for backtesting
start_date = date(2024, 1, 1)
end_date = date(2024, 1, 31)

# Run the strategy
strategy.run(start_date, end_date, OutSellStrategy)
