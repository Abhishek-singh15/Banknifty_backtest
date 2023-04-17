#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 21:56:12 2023

@author: abhisheksingh
"""

#importing libraries

import pandas as pd
import numpy as np
from datetime import datetime

from Order_matching_engine import get_orders,realtime_event



def check_risk(Final_portfolio):
    
    print("Risk_manager is running")
    
   
    
    
    #Checking KPI
    #This function can be further modified according to the need 
    
    if Final_portfolio['PNL'].sum()>=-5000000 and Final_portfolio['Sharpe'].mean()>-3:
        
        
        #Here risk_management related logic can be return
        
        
        print("Risk_manager is complete")
        
        
        return get_orders
    
    
    else:
        
        return
        
        
    
    
    
    
    
    
    
    
    
    
    
