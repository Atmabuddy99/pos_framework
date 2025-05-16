from genc import  GenericStrategy
import pandas as pd
import datetime


data_dir = r"D:\\ZFinal_Chains\\nifty_chain"  # Path to options chain data
expiry_list = r"D:\version3_with_date\sams_strategy\\expiries_nifty"


strategy = GenericStrategy(data_dir, expiry_list)

start_date = "2024-01-01"
end_date = "2024-02-28"


start_date=datetime.date(2024,1,1)
end_date=datetime.date(2024,2,1)

from new_strategy import OutSellStrategy

strategy.run(start_date=start_date,end_date=end_date,strategy_class=OutSellStrategy)

print("chamned fucker asshole ")