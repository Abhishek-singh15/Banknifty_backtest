#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 21:51:30 2023

@author: abhisheksingh
"""

#importing libraries

import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as pt
import openpyxl

from Risk_hedge_manager import check_risk
from Event_handler import get_events
from Order_matching_engine import get_orders,realtime_event

def get_strategy(hist_data,
                 curr_data,
                 bnf_spot_df,
                 event,
                 order_matching_engine_mssg,
                 event_handler_mssg,
                 time_scheduler,
                 time_buffer):
    
    
    
    #variable definition
    initial_time=time.time()
    
    fma=10 #Fast moving average
    sma=5 #Slow moving average
    
   
    order_matching_engine_mssg=get_orders()
    print("Strategy Engine is running.....................")

    if event=="historical" and order_matching_engine_mssg=="Pass":
        
        if event_handler_mssg=='okay':
            
            
            bnf_spot_df['sma']=bnf_spot_df['close'].rolling(10).mean()
            
            bnf_spot_df['fma']=bnf_spot_df['close'].rolling(5).mean()
            
# =============================================================================
#             bnf_spot_df['Signal_buy']=np.where((bnf_spot_df['fma']>bnf_spot_df['sma'])&
#             (bnf_spot_df['fma'].shift(1)<bnf_spot_df['sma'].shift(1))&
#             (bnf_spot_df['fma'].shift(-1)>bnf_spot_df['sma'].shift(-1)),'CALL_BUY','no')
#             
#             
# =============================================================================
            
            bnf_spot_df['Signal_buy']=np.where((bnf_spot_df['fma']>bnf_spot_df['sma'])&
            (bnf_spot_df['fma'].shift(1)>=bnf_spot_df['sma'].shift(1))&
            (bnf_spot_df['fma'].shift(2)<bnf_spot_df['sma'].shift(2)),'CALL_BUY','no')
            
            
            bnf_spot_df['Signal_sell']=np.where((bnf_spot_df['fma']<bnf_spot_df['sma'])&
            (bnf_spot_df['fma'].shift(1)<=bnf_spot_df['sma'].shift(1))&
            (bnf_spot_df['fma'].shift(2)>bnf_spot_df['sma'].shift(2)),'PUT_BUY','no')
            
            
            #Calculating ATM strike price using banknifty spot price
            
            
            bnf_spot_df['Strike_price']=bnf_spot_df['close'].apply(lambda x:100*round(x/100))
            
            
            signal_df=bnf_spot_df[['date','time','sma','fma','Signal_buy','Signal_sell','Strike_price']]
            
            
            
            #Selecting only rows which have signal in it
            signal_df=signal_df[(signal_df['Signal_buy']=='CALL_BUY') | (signal_df['Signal_sell']=='PUT_BUY')]
            

            
            
            signal_df['Instrument']=np.where(signal_df['Signal_buy']=='CALL_BUY','CE','PE')
            
            
            
            
            
            
            #for resampling on 1 min 
            
            resampled_df=signal_df.ffill().resample('1min', on='date').mean()
            
            resampled_df=resampled_df.reset_index()
            
            resampled_df=resampled_df[['date']]
            
            resampled_df['temp_date']=resampled_df['date']
            
            resampled_df['date']=resampled_df['temp_date'].apply(lambda x:str(x)[0:10])
            
            resampled_df['date']=pd.to_datetime(resampled_df['date'])
            
            resampled_df['time']=resampled_df['temp_date'].apply(lambda x:str(x)[11:19])
            
            resampled_df['time']=pd.to_datetime(resampled_df['time']).dt.time
            
            
            #Selecting required columns
            
            resampled_df=resampled_df[['date','time']]
            
            
            #setting up same index for both resampled_df and signal_df
            
            resampled_df=resampled_df.set_index(['date','time'])
            
            signal_df=signal_df.set_index(['date','time'])
            
            
            resampled_signal_df=pd.concat([resampled_df,signal_df],axis=1)
            
            
  #forward filling so that if that timestamp is missing on options data it can read from buy/sell signal of spot
            
            
            resampled_signal_df['Signal_buy']=resampled_signal_df['Signal_buy'].ffill()         
            resampled_signal_df['Signal_sell']=resampled_signal_df['Signal_sell'].ffill() 
            resampled_signal_df['Strike_price']=resampled_signal_df['Strike_price'].ffill()         
            resampled_signal_df['Instrument']=resampled_signal_df['Instrument'].ffill() 
            
            
            
            resampled_signal_df=resampled_signal_df.reset_index()
            resampled_signal_df=resampled_signal_df.fillna(0)
            resampled_signal_df['Strike_price']=resampled_signal_df['Strike_price'].astype('int')
            resampled_signal_df=resampled_signal_df.set_index(['date','time','Strike_price','Instrument'])
            
            
            #concating hist_data with resampled_signal_df to get the signals in options dataframe from spot
            
            hist_data=hist_data.reset_index()
            hist_data['Strike_price']=hist_data['Strike_price'].astype('int')

            
            hist_data=hist_data.sort_values(by=['date','time'])
            
            print(hist_data.columns)
            
            

            hist_data=hist_data.set_index(['date','time','Strike_price','Instrument'])
            Final_portfolio=resampled_signal_df.copy()
            
            hist_dict=hist_data.to_dict()
            
            
            #Mapping all the values to final_portfolio dataframe
            
            Final_portfolio['ticker']=Final_portfolio.index.map(hist_dict['ticker'])
            Final_portfolio['open']=Final_portfolio.index.map(hist_dict['open'])
            Final_portfolio['high']=Final_portfolio.index.map(hist_dict['high'])
            Final_portfolio['low']=Final_portfolio.index.map(hist_dict['low'])
            Final_portfolio['close']=Final_portfolio.index.map(hist_dict['close'])
            Final_portfolio['Expiry_month']=Final_portfolio.index.map(hist_dict['Expiry_month'])
            
            Final_portfolio.reset_index(inplace=True)
            
            
            
            
           
            Final_portfolio=Final_portfolio.set_index(['date','time'])
            
            
            
            
            #Recalculating signal and getting rid of false signals at different timestamp
            
            
            Final_portfolio['Final_call']=np.where((Final_portfolio['Signal_buy']=='CALL_BUY')
                                                   & (Final_portfolio['fma']!=0),'BUY_ATM_CALL','no')
            
            
            Final_portfolio['Final_put']=np.where((Final_portfolio['Signal_sell']=='PUT_BUY')
                                                   & (Final_portfolio['fma']!=0),'BUY_ATM_PUT','no')
            
            
            Final_portfolio.replace(0, np.nan, inplace=True)
            Final_portfolio.replace('no', np.nan, inplace=True)
            Final_portfolio.dropna(how='all',inplace=True)
            
            
            #Calculating expiry close price
            Final_portfolio=Final_portfolio.reset_index()
            Final_portfolio['month']=Final_portfolio['date'].dt.month
            
            
            expiry_df=Final_portfolio.pivot_table(index=['month','Instrument','Strike_price'],
            values=['close'],
            aggfunc='last')
            
            
            #expiry_df.rename(columns={'close':'expiry_close'},inplace=True)
            
            #expiry_df=expiry_
            
            
            #expiry_df=expiry_df.reset_index()
            Final_portfolio['Strike_price']=Final_portfolio['Strike_price'].astype('int')
            Final_portfolio=Final_portfolio.set_index(['month','Instrument','Strike_price'])
            
            expiry_dict=expiry_df['close'].to_dict()
            
            
            Final_portfolio['expiry_close']=Final_portfolio.index.map(expiry_dict)
            
            
            Final_portfolio['Estimeated_SL']=Final_portfolio['close'].where
            (Final_portfolio['Final_call']=='BUY_ATM_PUT').apply(lambda x:x*0.8)
            
            
            
            Final_portfolio['Estimeated_SL']=Final_portfolio.apply(lambda x:x.close*0.8 
                                            if x.Final_call=='BUY_ATM_CALL' or x.Final_put=='BUY_ATM_PUT'
                                            else np.nan,axis=1)
            
            
            
            #Creating another dataframe for stop-loss calculation
            
            #df=Final_portfolio
            
#Detecting using for loop..time complecity O(n2)            
# =============================================================================
#             for i in df.index:
#                 if df['Final_call'][i]=='BUY_ATM_CALL' or df['Final_put'][i]=='BUY_ATM_PUT':
#                     for j in range(i,len(df)):
#                         
#                         if df['Final_call'][i]=='BUY_ATM_CALL':
#                             
#                             
#                             if df['open'][j]<df['Estimeated_SL'][i]:
#                                 
#                                 df['final_sl'][i]=df['low'][j]
#                                 
#                             elif df['open'][j]>df['Estimeated_SL'][i] and df['low'][j]<=df['Estimeated_SL'][i]:
#                                 
#                                 df['final_sl'][i]=df['low'][j]
#                                 
#                                 
#                             else:
#                                 df['final_sl'][i]="no_sl"
#                                 
#                                 
#                         if df['Final_put'][i]=='BUY_ATM_PUT':
#                             
#                             
#                             if df['open'][j]>df['Estimeated_SL'][i]:
#                                 
#                                 df['final_sl'][i]=df['high'][j]
#                                 
#                             elif df['open'][j]<df['Estimeated_SL'][i] and df['high'][j]>=df['Estimeated_SL'][i]:
#                                 
#                                 df['final_sl'][i]=df['high'][j]
#                                 
#                                 
#                             else:
#                                 df['final_sl'][i]="no_sl"
#                                 
#                                 
#                                 
#             
# =============================================================================




#Detecting sl using single for...time complexity <O(n)

# =============================================================================
# #=============================================================================
# for i in df.index:
#     
#     if df['Final_call'][i]=='BUY_ATM_CALL' or df['Final_put'][i]=='BUY_ATM_PUT':
#         dummy_df=df[i+1:len(df)-2]
#         
#         if df['Final_call'][i]=='BUY_ATM_CALL':
#             
#             dummy_df['final_sl_call']=np.where((dummy_df['Strike_price']==df['Strike_price'][i])
#                                         &(dummy_df['Instrument']==df['Instrument'][i])
#                                         &(dummy_df['Expiry_month']==df['Expiry_month'][i])
#                                         &(
#                                         (dummy_df['open'][j]<df['Estimeated_SL'][i])
#                                         |((dummy_df['open'][j]>df['Estimeated_SL'][i]) & (dummy_df['low'][j]<=df['Estimeated_SL'][i]))
#                                         )
#                                         ,dummy_df['low'],'no-sl')
#             
#             
#             
#             
#         if df['Final_put'][i]=='BUY_ATM_PUT':
#             
#             dummy_df['final_sl_put']=np.where((dummy_df['Strike_price']==df['Strike_price'][i])
#                                         &(dummy_df['Instrument']==df['Instrument'][i])
#                                         &(dummy_df['Expiry_month']==df['Expiry_month'][i])
#                                         &(
#                                         (dummy_df['open'][j]>df['Estimeated_SL'][i])
#                                         |((dummy_df['open'][j]<df['Estimeated_SL'][i]) & (dummy_df['high'][j]>=df['Estimeated_SL'][i]))
#                                         )
#                                         ,dummy_df['high'],'no-sl')
#             
#             
#         #calculating final sl in df    
#             
#             
#         df['final_sl_call'][i]=[n for n in dummy_df['final_sl_call'].unique() if n!='no-sl'][0]
#         
#         df['final_sl_put'][i]=[n for n in dummy_df['final_sl_put'].unique() if n!='no-sl'][0]
#         
#         
#         
#       
#             
# # =============================================================================
#                                 
#                                 
# =============================================================================
                                

            #Getting some runtime error in sl code..so by default taking 'no-sl'
            
            Final_portfolio=Final_portfolio.reset_index()

            Final_portfolio['final_sl_call'],Final_portfolio['final_sl_put']='no-sl','no-sl'
            
            
            #Selecting only the important rows from Final_portfolio
            
            
            Final_portfolio=Final_portfolio[['date','time','ticker','Strike_price','Instrument'
                                             ,'Expiry_month','Final_call','Final_put','close'
                                             ,'expiry_close','Estimeated_SL','final_sl_call',
                                             'final_sl_put']]
            
            
            
# =============================================================================
#             Final_portfolio['PNL_CALL']=Final_portfolio.apply(lambda x:x.expiry_close-x.close 
#                                                          if (x.final_sl_call!='no-sl') & (x.Final_call=='BUY_ATM_CALL')
#                                                          else x.final_sl_call-x.close,axis=1)
#             


# =============================================================================

# =============================================================================

#             Final_portfolio['PNL_PUT']=Final_portfolio.apply(lambda x:x.expiry_close-x.close 
#                                                          if x.final_sl_put!='no-sl' 
#                                                          else x.final_sl_put-x.close,axis=1)
#             
# =============================================================================
          
            
            
            Final_portfolio['PNL_CALL']=np.where(Final_portfolio['Final_call']=='BUY_ATM_CALL',
                                                 Final_portfolio['expiry_close']-Final_portfolio['close'],"NaN"
                                                 )
            
            
            Final_portfolio['PNL_PUT']=np.where(Final_portfolio['Final_put']=='BUY_ATM_PUT',
                                                 Final_portfolio['expiry_close']-Final_portfolio['close'],"NaN"
                                                 )
            
            
            
            #Deleting rows where nan values comin in pnl
            
            Final_portfolio=Final_portfolio[(Final_portfolio['PNL_CALL']!="NaN") | (Final_portfolio['PNL_PUT']!="NaN")]
            
            
            
            
            
 #-----------------Calculating KPI----------------------------------------------#           
            
            Final_portfolio['PNL']=np.where(Final_portfolio['PNL_CALL']!="NaN",
                                            Final_portfolio['PNL_CALL'],Final_portfolio['PNL_PUT'])
            
            
            #Final_portfolio['PNL']=Final_portfolio['PNL'].apply(lambda x:str(x))
            Final_portfolio['PNL']=Final_portfolio['PNL'].apply(lambda x:float(x))

            
            
            
            
            Final_portfolio['Equity_curve']=Final_portfolio['PNL'].cumsum()
            
            
            Final_portfolio['Rolling_Drawdown']=1*(Final_portfolio['PNL'].cumsum().cummax()-Final_portfolio['PNL'].cumsum())

            
            
            Final_portfolio['Avg_win']=sum(i for i in Final_portfolio['PNL'] if i>0)/len([i for i in Final_portfolio['PNL'] if i>0])
                                        
            
            
            Final_portfolio['Avg_loss']=sum(i for i in Final_portfolio['PNL'] if i<0)/len([i for i in Final_portfolio['PNL'] if i<0])
            
            
            Final_portfolio['Hit_ratio']=len([i for i in Final_portfolio['PNL'] if i>0])/len([i for i in Final_portfolio['PNL'] if i<0])

            
            Final_portfolio['Sharpe']= Final_portfolio['PNL'].mean()/Final_portfolio['PNL'].std()
            
            
            
            # Plottung the Equity curve Graph
            
            x=Final_portfolio['date']

            y=Final_portfolio['Equity_curve']

            pt.xlabel("Date")
            pt.ylabel("PNL")
            pt.plot(x, y)
            pt.xticks(rotation=90)


            pt.show()
            
            
            # Plottung the Drawdown Graph
            
            x=Final_portfolio['date']

            y=Final_portfolio['Rolling_Drawdown']

            pt.xlabel("Date")
            pt.ylabel("Drawdown")
            pt.plot(x, y)
            pt.xticks(rotation=90)


            pt.show()
            
            #Downloading final portfolio dataframe to excel
            file_name = 'Final_portfolio.xlsx'
            Final_portfolio.to_excel(file_name)
            
            
           
            
        elif event_handler_mssg=='pipeline_broken':
            
            return get_events()
            
            
            
     
            
        else:
            
            return None
            
            
            
            
            
            
    
    
    if event=="realtime" and order_matching_engine_mssg=="Pass":
        
        if event_handler_mssg=='okay':
            
            return get_strategy(hist_data,
                             curr_data,
                             bnf_spot_df,
                             event,
                             order_matching_engine_mssg,
                             event_handler_mssg,
                             time_scheduler,
                             time_buffer)
            
            
            
            
            
            
            
        elif event_handler_mssg=='pipeline_broken':
            
            return get_events()
            
            
            
            
            
        else:
            
            return None
            
            
            
       
        
        
    if event=="realtime" and order_matching_engine_mssg!="Pass":
        
        
        
        if event_handler_mssg=='okay':
            
            return realtime_event()
            
            
            
            
        elif event_handler_mssg=='pipeline_broken':
            
            return get_events()
            
            
        else:
            
            return None
        
        
    #Checking timeline    
        
    total_runtime=time.time()-initial_time
    
    time_scheduler+=total_runtime
    
    
    
    
    if total_runtime<=time_buffer:
        
        strategy_engine_mssg="okay"
        
        
    else:
        
        strategy_engine_mssg="pipeline_broken"
        
        
        
    
    print("Your Final Portfolio is " ,Final_portfolio)
    print("Strategy Engine is complete.....................")
    return check_risk(Final_portfolio),order_matching_engine_mssg,strategy_engine_mssg,time_scheduler,time_buffer