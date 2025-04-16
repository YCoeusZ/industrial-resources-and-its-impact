#This file is designed to produce the same result as the ipynb file with the same name (i.e. create pro-change data for tech index, and updated file "../../processed/his_index_tech_500_stable_14_24.csv"). 

import pandas as pd

df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")

df_his_index["Date"]=pd.to_datetime(df_his_index["Date"])

lst_time_delta=[0]
for index in range(1,2568): 
    lst_time_delta.append((df_his_index.loc[index]["Date"]-df_his_index.loc[index-1]["Date"]).days)
    
df_his_index["Timedelta_days"]=lst_time_delta

lst_pro_change=[(df_his_index.loc[0]["Close_wi"]-df_his_index.loc[0]["Open_wi"])*100/df_his_index.loc[0]["Open_wi"]]

for index in range(1,2568): 
    lst_pro_change.append((df_his_index.loc[index]["Close_wi"]-df_his_index.loc[index-1]["Close_wi"])*100/df_his_index.loc[index-1]["Close_wi"])
    
df_his_index["pro_change_close_wi"]=lst_pro_change

df_his_index.at[737,"pro_change_close_wi"]=(df_his_index.loc[737]["Close_wi"]-df_his_index.loc[737]["Open_wi"])*100/df_his_index.loc[737]["Open_wi"]

df_his_index.to_csv("../../processed/his_index_tech_500_stable_14_24.csv", index=False)
