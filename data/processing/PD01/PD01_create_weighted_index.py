# This file is designed to produce the same result as the ipynb file with the same name (i.e. create tech (stock) index and produce file "../../processed/his_weight_tech_500_14_24_stable.csv").

import pandas as pd

df_tech_500=pd.read_csv("../../processed/tech_sp500_stable_extended_tickers.csv")
df_tech_500_14_24=df_tech_500[df_tech_500["stable 14 to 24"]==True]
lst_tech_extic_str=list(df_tech_500_14_24["extended tickers"])
lst_tech_extic_set=[set(ele_str.split(",")) for ele_str in lst_tech_extic_str]

set_tech_extic=set()

for ele_set in lst_tech_extic_set: 
    set_tech_extic=set_tech_extic.union(ele_set)
    
df_tech_his_weight=pd.read_csv("../../processed/his_weight_tech_500_12_24_in_progress.csv")

df_tech_his_weight_14_24=df_tech_his_weight[df_tech_his_weight.date>"2013-12-31"]

df_tech_his_weight_14_24_stable=df_tech_his_weight_14_24[df_tech_his_weight_14_24.ticker.isin(set_tech_extic)]

df_tech_his_weight_14_24_stable.to_csv("../../processed/his_weight_tech_500_14_24_stable.csv", index=False)
df_tech_his_weight_14_24_stable=pd.read_csv("../../processed/his_weight_tech_500_14_24_stable.csv") # so that the index is fixed. 

lst_dates=list(df_tech_his_weight_14_24_stable.date.unique())

lst_sum_tech_weight=[sum(df_tech_his_weight_14_24_stable[df_tech_his_weight_14_24_stable.date == str_date].weight) for str_date in lst_dates]

df_tech_weights=pd.DataFrame({"date":lst_dates, "tech_weight":lst_sum_tech_weight})

df_his_index=pd.read_csv("../../raw/stock/SandP500_index_2009_2025.csv")

df_his_index=df_his_index.iloc[::-1] # reversing the dataframe to the sake of it 

date_dt=pd.to_datetime(df_his_index.Date)

date_str=[str(date.to_pydatetime())[:10] for date in date_dt]

df_his_index["Date"]=date_str

df_his_index_14_24=df_his_index[df_his_index["Date"].isin(lst_dates)]

lst_dates_index=list(df_his_index[df_his_index["Date"].isin(lst_dates)].Date)

lst_miss_date=[]

for date in lst_dates: 
    if date not in lst_dates_index: 
        lst_miss_date.append(date)
        
df_tech_weights_nm=df_tech_weights[~df_tech_weights["date"].isin(lst_miss_date)]

df_tech_weights_nm=df_tech_weights_nm.rename(columns={"date":"Date"})

df_tech_weights_index=df_tech_weights_nm.merge(df_his_index_14_24,on="Date",how="inner")

df_tech_weights_index["Open_wi"]=df_tech_weights_index["tech_weight"]*df_tech_weights_index[" Open"]/100

df_tech_weights_index["High_wi"]=df_tech_weights_index["tech_weight"]*df_tech_weights_index[" High"]/100

df_tech_weights_index["Low_wi"]=df_tech_weights_index["tech_weight"]*df_tech_weights_index[" Low"]/100

df_tech_weights_index["Close_wi"]=df_tech_weights_index["tech_weight"]*df_tech_weights_index[" Close"]/100

df_tech_weights_index.to_csv("../../processed/his_index_tech_500_stable_14_24.csv",index=False)

