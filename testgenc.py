from genc import  GenericStrategy
import pandas as pd



#obj=GenericStrategy


data_dir = "e:/nifty_chain"  # Path to options chain data
expiry_list = "e:/my_coding/expiries_nifty"  # Path to expiry list
    
    # Create generic strategy instance
strategy = GenericStrategy(data_dir, expiry_list)


start_date = "2024-01-01"
end_date = "2024-02-28"
import datetime

start_date=datetime.date(2024,1,1)
end_date=datetime.date(2025,2,1)
class_=1
from new_strategy import OutSellStrategy
strategy.run(start_date=start_date,end_date=end_date,strategy_class=OutSellStrategy)

print("completed")
empty_df=[]

print(strategy.all_tradebooks)

for strat in strategy.all_tradebooks:
    print("here")
    #orderbook1 = pd.DataFrame(strat.tb.orders)
    orderbook1 = pd.DataFrame(strat._all_trades)
    print(orderbook1)
    empty_df.append(orderbook1)

final_df = pd.concat(empty_df, ignore_index=True) if empty_df else pd.DataFrame()


final_df.to_csv("strategy_positional.csv")


