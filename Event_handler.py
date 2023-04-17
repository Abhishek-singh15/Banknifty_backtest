#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 21:54:42 2023

@author: abhisheksingh
"""


#importing libraries

import pandas as pd
import numpy as np
from datetime import datetime
import glob
import time


from Order_matching_engine import get_orders,realtime_event










# function definition

def get_events(
        from_timestamp,
                   to_timestamp,
                       connection,
                       exchange_audit_trail,
                       autotrader_mssg,
                       time_scheduler,
                       time_buffer,
               ):
    # specifying the path to csv files in a path_dictionary
    
    print("Event Handler is starting.....................")
    

    path_dict={'path_jan':r"/Users/abhisheksingh/Documents/Data/root/January",
               'path_feb':r"/Users/abhisheksingh/Documents/Data/root/February",
               'path_mar':r"/Users/abhisheksingh/Documents/Data/root/March",
               
               }

    path_bnknifty_spot=r"/Users/abhisheksingh/Documents/Data/root/bnf_spot.csv"




    
    initial_time=time.time()
    
    
    #Getting current timestamp to know whether to run historical events or realtime events
    
    curr_timestamp=time.time()
    
    # Passing value to order_matching engine to bypass historical events
    order_matching_engine_mssg='pass'
    
    #Creating historical dataframe and banknifty spot dataframe
    
    hist_data=pd.DataFrame()
    curr_data=pd.DataFrame()
    bnf_spot_df=pd.DataFrame()
    #Creating expiry month list
    month=['JAN','FEB','MAR','APR','MAY','JUN']
    
   
    
    
    
    #Checking if the event is historical or realtime
    
    
    if curr_timestamp>=int(to_timestamp[0]):
        
        #order_matching_engine message should be None beacuse we are dealing with historical data
        
        order_matching_engine_mssg=None
        
        event="historical"
        
        
        
        for i in path_dict.values():
            
            print("Please wait files are loading.......")
            
            files= glob.glob(i + "/*.csv")
            
            df_folder=pd.DataFrame()
            
            for file_name in files:
                
            
                df=pd.read_csv(file_name)
                file_name=file_name.replace(i,"")
                
              
                file_name=file_name.replace(".csv","")
                
                
                #df['Strike_price']=df['<ticker>'].apply(lambda x:x[14:19])
                df['Expiry_month']=df['<ticker>'].apply(lambda x:x[11:14])
                
                df=df[df['Expiry_month'].isin(month)] 
                
                df['Strike_price']=df['<ticker>'].apply(lambda x:x[14:19])
                
                df['Instrument']=df['<ticker>'].apply(lambda x:x[len(x)-2:])
                
                df_folder=pd.concat([df_folder,df])
                
                
         
                
            
                
            
            hist_data=pd.concat([hist_data,df_folder])
            
        bnf_spot_df=pd.read_csv(path_bnknifty_spot)
            
            
            
            
        #--------------Datetime Conversion--------------------#
        
        
        hist_data['<date>']=pd.to_datetime(hist_data['<date>'],format='%m/%d/%Y')
        hist_data['<time>']=pd.to_datetime(hist_data['<time>'],format='%H:%M:%S').dt.time
        
        
        
        bnf_spot_df['temp_date']=bnf_spot_df['date']
        
        bnf_spot_df['date']=bnf_spot_df['temp_date'].apply(lambda x:x[0:10])
        bnf_spot_df['date']=pd.to_datetime(bnf_spot_df['date'],format='%Y/%m/%d')
        
        bnf_spot_df['time']=bnf_spot_df['temp_date'].apply(lambda x:x[11:19])
        bnf_spot_df['time']=pd.to_datetime(bnf_spot_df['time'],format='%H:%M:%S').dt.time
        
        
        bnf_spot_df=bnf_spot_df.drop(['temp_date'],axis=1)
        
        #Renaming hist_data columns so that its similar to bnf_Spot
        
        hist_data.rename(columns={'<ticker>':'ticker',
                                            '<date>':'date',
                                            '<time>':'time',
                                            '<open>':'open',
                                            '<high>':'high',
                                            '<low>':'low',
                                            '<close>':'close',
                                            '<volume>':'volume',
                                            '<o/i> ':'o/i'},inplace=True
                                   )
        
       
        
        
        #Checking for NaN values in hist_data and bnf_spot and forward fill if find any
        
        if hist_data.isnull().values.any():
            
            hist_data=hist_data.ffill(axis=0)
            
            #if nan is in first row
            
            hist_data=hist_data.bfill(axis=0)
            
            
        if bnf_spot_df.isnull().values.any():
            
              
            bnf_spot_df=bnf_spot_df.ffill(axis=0)
              
              #if nan is in first row
              
            bnf_spot_df=bnf_spot_df.bfill(axis=0)
            
            
            
        # Making sure that banknifty spot has same dates as of hist_data
        
        dates=hist_data['date'].unique()
        
        bnf_spot_df=bnf_spot_df[bnf_spot_df['date'].isin(dates)]
        
        
        
            
        

        
        
        
    else:
        
        event="realtime"
   
        
        
        curr_data,bnf_spot_df,order_matching_engine_mssg=realtime_event(path_dict)
        
        
        
        
        
        
        
        
    total_runtime=time.time()-initial_time
    
    time_scheduler=int(time_scheduler[0]) + total_runtime
    
    
    
    
    if total_runtime<=time_buffer:
        
        event_handler_mssg="okay"
        
        
    else:
        
        event_handler_mssg="pipeline_broken"
        
        
      
        
        
 
    
     
    print("Event Handler is complete.....................")
    return hist_data,curr_data,bnf_spot_df,event,order_matching_engine_mssg,event_handler_mssg,time_scheduler,time_buffer
        
        
    
        

    
    
