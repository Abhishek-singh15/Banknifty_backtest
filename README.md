# Banknifty_backtest
This repository contains 5 files namely:-

1)Backtest_autotrader
2)Event_handler
3)Strategy_engine
4)Order_matching_engine
5)Risk_hedge_manager


Below is the steps to run the file and see the results:

Step - 1) Create a folder and paste these files to it
Step - 2) Goto Event-handler script and specify the path to path_dict(for options data) and to path_bnknifty_spot.. [Note...it is important that files are saved in similar fashion as provided in code]
Step - 3) Goto Backtest_autotrader and run the script
Step - 4) After successfull run an excel file will be saved in your current folder...access this file to see backtested results
Step - 5) Check Console to see Equity Curve and Drawdown graphs

----------------------Note->I have added print statements wherever necessary so that you can track the overall flow of program----------------------------


Brief discription of each script

1)Backtest_autotrader - This script is like a skeleton of this whole program. It is managing the pipeline or flow of scripts. This script is responsible to directly connect with exchange and run your algo on prod servers. I have introduced time_scheduler and time_buffer variables to keep track of time.
Time_scheduler variable : keeps track of how much time every script is taking.


2)Event_handler - This script handles the events which are classified into Realtime and historical. All the historical backtesting comes in histoorical event. All realtime events(tick by tick data or continuous stream of data or realtime data) backtesting is handled by realtime event part of event handler.


3)Strategy_engine - This script gets data from Event_handler and runs the strategy whatever you have provided. If strategy is based on realtime events ,it maintains connection with order matching engine as well.


4)Order_matching_engine - This script maintains connection with all other scripts and keep informing them about the realtime events of exchange, limit, market orders. It also informs the strategy engine that getting a fill based on strategy may or may not be possible.


5)Risk_hedge_manager - This script is built to manage the risk of portfolio. Currently I have added only total sum and sharpe of your portfilio as a risk managing parameter. One can add VAR related logic as well.



