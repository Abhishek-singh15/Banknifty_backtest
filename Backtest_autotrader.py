#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 21:47:47 2023

@author: abhisheksingh
"""


#-----------------Autotrader for backtesting strategy-----------------#
#importing libraries

import pandas as pd
import numpy as np
from datetime import datetime
import glob
import time
import sys
import urllib.request
#For running backtest on tick by tick..currently I havem't used it
import asyncio
# =============================================================================
from Event_handler import get_events
from Strategy_engine import get_strategy
from Risk_hedge_manager import check_risk
from Order_matching_engine import get_orders,realtime_event
# 
# 
# =============================================================================

#To track time
initial_time=time.time()


#Continuously checking if the connection is made with exchange...here I have used google as example for exchange

def connect(host='http://google.com'):
    try:
        urllib.request.urlopen(host) #Python 3.x
        return True
    except:
        return False




if connect():
    
    from_timestamp=None,
    #by default giving current time...you can give according to your choice
    to_timestamp=time.time(),
    connection=True,
    exchange_audit_trail= None,
    autotrader_mssg= "okay" ,
    time_scheduler=time.time(),
    time_buffer=sys.maxsize

# First running the event handler

hist_data,curr_data,bnf_spot_df,event,order_matching_engine_mssg,event_handler_mssg,time_scheduler,time_buffer=get_events(from_timestamp,to_timestamp,
               connection,
               exchange_audit_trail,
               autotrader_mssg,
               time_scheduler,
               time_buffer,
               )






if event=="historical":
    
    risk_mssg,order_matching_engine_mssg,strategy_engine_mssg,time_scheduler,time_buffer=get_strategy(hist_data,
                     curr_data,
                     bnf_spot_df,
                     event,
                     order_matching_engine_mssg,
                     event_handler_mssg,
                     time_scheduler,
                     time_buffer)
    
    
    
    
    
   
    
    if risk_mssg=='Pass' and strategy_engine_mssg!="pipeline_broken":
        
        get_orders()
        
        
    
    
    
if event=="realtime":
    
    if order_matching_engine_mssg=="Pass":
        
        realtime_event()
        
        
        
print("Total time of backtest and Trade Execution in seconds",time_scheduler-initial_time)

print("Autotrader complete...........")




