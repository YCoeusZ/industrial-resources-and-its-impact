#This file is designed to produce the same result as "PD03_processing_cop.ipynb", "PD03_processing_daily_data_oil.ipynb", "PD03_processing_fed_rates.ipynb", "PD03_processing_gold.ipynb", "PD03_processing_pal.ipynb", "PD03_processing_plat.ipynb", and "PD03_processing_silv.ipynb" (i.e. processing daily macro values and produce files "../../processed/cop_daily_we_care.csv", "../../processed/crude_oil_we_care.csv", "../../processed/fed_rate_we_care.csv", "../../processed/gold_daily_we_care.csv", "../../processed/pal_daily_we_care.csv", "../../processed/plat_daily_we_care.csv", and "../../processed/silv_daily_we_care.csv")

#PD03_processing_cop.ipynb
import pandas as pd
import numpy as np

import sys
sys.path.append("../../../")
from proj_mod import fill_in_linearly

df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")
lst_date_we_care=df_his_index["Date"].to_list()
df_cop=pd.read_csv("../../raw/macro_daily/copper-prices-historical-chart-data.csv")
df_cop_care=df_cop[df_cop["date"].isin(lst_date_we_care)]
lst_missing=list(set(lst_date_we_care)-set(df_cop_care["date"].to_list()))
df_cop_data=pd.DataFrame({"date": lst_date_we_care})
df_cop_data=df_cop_data.join(df_cop_care.set_index("date"), how="left", on="date")
df_cop_data["date"]=pd.to_datetime(df_cop_data["date"])

fill_in_linearly.v_datetime(df_cop_data,"date"," value")

lst_pro_change=[(np.sqrt(df_cop.loc[13631][" value"]/df_cop.loc[13630][" value"])-1)*100]
for index in range(1,2568): 
    lst_pro_change.append((df_cop_data.loc[index][" value"]/df_cop_data.loc[index-1][" value"]-1)*100)
    
df_cop_data["pro_change"]=lst_pro_change

df_cop_data.at[737,"pro_change"]=(df_cop_data.loc[737][" value"]/df_cop.loc[14510][" value"]-1)*100

df_cop_data.to_csv("../../processed/cop_daily_we_care.csv")

#PD03_processing_daily_data_oil.ipynb
df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")
lst_str_date_care=df_his_index["Date"].to_list()
df_oil=pd.read_csv("../../raw/macro_daily/Crude Oil WTI Futures Historical Data (2).csv")
df_oil_care=df_oil[df_oil["Date"].isin(lst_str_date_care)]
df_oil_care=df_oil_care[::-1] # reversing the df
df_oil_care=df_oil_care.reset_index()
lst_pro_change=[(df_oil_care.loc[0]["Price"]-df_oil_care.loc[0]["Open"])*100/df_oil_care.loc[0]["Open"]]
for index in range(1,2568):
    lst_pro_change.append((df_oil_care.loc[index]["Price"]-df_oil_care.loc[index-1]["Price"])*100/df_oil_care.loc[index-1]["Price"])
df_oil_care["pro_change"]=lst_pro_change
df_oil_care.at[737,"pro_change"]=(df_oil_care.loc[737]["Price"]-df_oil_care.loc[737]["Open"])*100/df_oil_care.loc[737]["Open"]
df_oil_care["Date"]=pd.to_datetime(df_oil_care["Date"])
df_his_index["Date"]=pd.to_datetime(df_his_index["Date"])
df_oil_care.to_csv("../../processed/crude_oil_we_care.csv")

#PD03_processing_fed_rates.ipynb
df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")
lst_date_we_care=df_his_index["Date"].to_list()
df_fed=pd.read_csv("../../raw/macro_daily/DFF.csv")
df_fed_care=df_fed[df_fed["observation_date"].isin(lst_date_we_care)]
df_fed_care.to_csv("../../processed/fed_rate_we_care.csv")

#PD03_processing_gold.ipynb
df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")
df_his_index["Date"]=pd.to_datetime(df_his_index["Date"])
lst_date_we_care=df_his_index["Date"].to_list()
df_gold_1=pd.read_csv("../../raw/macro_daily/gold_and_silver_kaggle/gold prices.csv")
df_gold_1["Date"]=pd.to_datetime(df_gold_1["Date"])
df_gold_1=df_gold_1.drop("Volume", axis=1)
df_gold_2=pd.read_csv("../../raw/macro_daily/gold_and_silver_kaggle/Gold prices (18.08.2023 - 22.01.2024).csv")
df_gold_2["Date"]=pd.to_datetime(df_gold_2["Date"])
df_gold_2=df_gold_2.drop("Volume", axis=1)
df_gold_3=pd.read_csv("../../raw/macro_daily/gold_and_silver_kaggle/Gold Futures Historical Data (23.01.24-22.11.24).csv")
df_gold_3["Date"]=pd.to_datetime(df_gold_3["Date"])
df_gold_3=df_gold_3.drop(["Vol.","Change %"],axis=1)
df_gold_3=df_gold_3.rename(columns={"Price":"Close/Last"})
df_gold=pd.concat([df_gold_1,df_gold_2,df_gold_3])
df_gold=df_gold.sort_values("Date")
df_gold=df_gold.reset_index().drop("index",axis=1)
df_gold_care=df_gold[df_gold["Date"].isin(lst_date_we_care)].reset_index().drop("index",axis=1)

for col in ["Close/Last", "Open", "High", "Low"]: 
    for index in range(2568): 
        # print(index)
        if type(df_gold_care.loc[index][col])==str:
            df_gold_care.at[index,col]=float(df_gold_care.loc[index][col].replace(",",""))

lst_pro_change=[(df_gold_care.loc[df_gold_care["Date"]=="2014-01-02"]["Close/Last"].values[0]/df_gold_care.loc[df_gold_care["Date"]=="2014-01-02"]["Open"].values[0]-1)*100]

for index in range(1,2568): 
    lst_pro_change.append((df_gold_care.loc[index]["Close/Last"]/df_gold_care.loc[index-1]["Close/Last"]-1)*100)

df_gold_care["pro_change"]=lst_pro_change
df_gold_care.at[df_gold_care[df_gold_care["Date"]=="2017-07-06"].index[0],"pro_change"]=(df_gold_care.loc[df_gold_care["Date"]=="2017-07-06"]["Close/Last"].values[0]/df_gold_care.loc[df_gold_care["Date"]=="2017-07-06"]["Open"].values[0]-1)*100
df_gold_care.to_csv("../../processed/gold_daily_we_care.csv")

#PD03_processing_pal.ipynb
df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")
lst_date_we_care=df_his_index["Date"].to_list()
df_pal=pd.read_csv("../../raw/macro_daily/palladium-prices-historical-chart-data.csv")
df_pal_care=df_pal[df_pal["date"].isin(lst_date_we_care)]
lst_missing=list(set(lst_date_we_care)-set(df_pal_care["date"].to_list()))
df_pal_data=pd.DataFrame({"date": lst_date_we_care})
df_pal_data=df_pal_data.join(df_pal_care.set_index("date"), how="left", on="date")
df_pal_data["date"]=pd.to_datetime(df_pal_data["date"])
fill_in_linearly.v_datetime(df_pal_data, "date", " value")
lst_pro_change=[(np.sqrt(df_pal[df_pal["date"]=="2014-01-02"][" value"].values[0]/df_pal[df_pal["date"]=="2013-12-31"][" value"].values[0])-1)*100]

for index in range(1, 2568): 
    lst_pro_change.append((df_pal_data.loc[index][" value"]/df_pal_data.loc[index-1][" value"]-1)*100)
    
df_pal_data["pro_change"]=lst_pro_change
df_pal_data.at[737, "pro_change"]=(df_pal_data[df_pal_data["date"]=="2017-07-06"][" value"].values[0]/df_pal[df_pal["date"]=="2017-07-05"][" value"].values[0]-1)*100
df_pal_data.to_csv("../../processed/pal_daily_we_care.csv")

#PD03_processing_plat.ipynb
df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")
lst_date_we_care=df_his_index["Date"].to_list()
df_plat=pd.read_csv("../../raw/macro_daily/platinum-prices-historical-chart-data.csv")
df_plat_care=df_plat[df_plat["date"].isin(lst_date_we_care)]
lst_missing=list(set(lst_date_we_care)-set(df_plat_care["date"].to_list()))
df_plat_data=pd.DataFrame({"date": lst_date_we_care})
df_plat_data=df_plat_data.join(df_plat_care.set_index("date"), how="left", on="date")
df_plat_data["date"]=pd.to_datetime(df_plat_data["date"])
fill_in_linearly.v_datetime(df_plat_data, "date", " value")
lst_pro_change=[(np.sqrt(df_plat[df_plat["date"]=="2014-01-02"][" value"].values[0]/df_plat[df_plat["date"]=="2013-12-31"][" value"].values[0])-1)*100]

for index in range(1, 2568): 
    lst_pro_change.append((df_plat_data.loc[index][" value"]/df_plat_data.loc[index-1][" value"]-1)*100)
    
df_plat_data["pro_change"]=lst_pro_change
df_plat_data.at[df_plat_data[df_plat_data["date"]=="2017-07-06"].index[0], "pro_change"]=(df_plat_data[df_plat_data["date"]=="2017-07-06"][" value"].values[0]/df_plat[df_plat["date"]=="2017-07-05"][" value"].values[0]-1)*100
df_plat_data.to_csv("../../processed/plat_daily_we_care.csv")

#PD03_processing_silv.ipynb
df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")
df_his_index["Date"]=pd.to_datetime(df_his_index["Date"])
lst_date_we_care=df_his_index["Date"].to_list()
df_silv_1=pd.read_csv("../../raw/macro_daily/gold_and_silver_kaggle/silver prices.csv")
df_silv_1["Date"]=pd.to_datetime(df_silv_1["Date"])
df_silv_1=df_silv_1.drop("Volume", axis=1)
df_silv_2=pd.read_csv("../../raw/macro_daily/gold_and_silver_kaggle/Silver prices (18.08.2023 - 22.01.2024).csv")
df_silv_2["Date"]=pd.to_datetime(df_silv_2["Date"])
df_silv_2=df_silv_2.drop("Volume", axis=1)
df_silv_3=pd.read_csv("../../raw/macro_daily/gold_and_silver_kaggle/Silver Futures Historical Data (23.01.24-22.11.24).csv")
df_silv_3["Date"]=pd.to_datetime(df_silv_3["Date"])
df_silv_3=df_silv_3.drop(["Vol.","Change %"],axis=1)
df_silv_3=df_silv_3.rename(columns={"Price":"Close/Last"})
df_silv=pd.concat([df_silv_1,df_silv_2,df_silv_3])
df_silv=df_silv.sort_values("Date")
df_silv=df_silv.reset_index().drop("index",axis=1)
df_silv_care=df_silv[df_silv["Date"].isin(lst_date_we_care)].reset_index().drop("index",axis=1)
lst_pro_change=[(df_silv_care.loc[df_silv_care["Date"]=="2014-01-02"]["Close/Last"].values[0]/df_silv_care.loc[df_silv_care["Date"]=="2014-01-02"]["Open"].values[0]-1)*100]

for index in range(1,2568): 
    lst_pro_change.append((df_silv_care.loc[index]["Close/Last"]/df_silv_care.loc[index-1]["Close/Last"]-1)*100)
    
df_silv_care["pro_change"]=lst_pro_change
df_silv_care.at[df_silv_care[df_silv_care["Date"]=="2017-07-06"].index[0],"pro_change"]=(df_silv_care.loc[df_silv_care["Date"]=="2017-07-06"]["Close/Last"].values[0]/df_silv_care.loc[df_silv_care["Date"]=="2017-07-06"]["Open"].values[0]-1)*100
df_silv_care=df_silv_care.drop(["High","Low"],axis=1)
df_silv_care.to_csv("../../processed/silv_daily_we_care.csv")