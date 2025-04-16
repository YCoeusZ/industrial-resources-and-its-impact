# This file is designed to produce the same result as "pd05_including_daily_values.ipynb", "pd05_including_monthly_values.ipynb", and "pd05_considering_inflation.ipynb" (i.e. producing main datasets, and files "../../processed/all_data_collection.csv", and "../../processed/all_data_inf_adj_collection.csv"). 

#pd05_including_daily_values.ipynb
import pandas as pd
import numpy as np

import sys
sys.path.append("../../../")
from proj_mod import relative_period

df_his_index=pd.read_csv("../../processed/his_index_tech_500_stable_14_24.csv")

df_his_index["Date"]=pd.to_datetime(df_his_index["Date"])

lst_dates_care=df_his_index["Date"].to_list()

df_collection=pd.DataFrame({"Date":lst_dates_care})

df_collection["tech_index_pro_change"]=df_his_index["pro_change_close_wi"]

df_cop=pd.read_csv("../../processed/cop_daily_we_care.csv")

df_collection["cop_daily_pro_change"]=df_cop["pro_change"]

df_gold=pd.read_csv("../../processed/gold_daily_we_care.csv")

df_collection["gold_daily_pro_change"]=df_gold["pro_change"]

df_silv=pd.read_csv("../../processed/silv_daily_we_care.csv")

df_collection["silv_daily_pro_change"]=df_silv["pro_change"]

df_pal=pd.read_csv("../../processed/pal_daily_we_care.csv")

df_collection["pal_daily_pro_change"]=df_pal["pro_change"]

df_plat=pd.read_csv("../../processed/plat_daily_we_care.csv")

df_collection["plat_daily_pro_change"]=df_plat["pro_change"]

df_crude_oil=pd.read_csv("../../processed/crude_oil_we_care.csv")

df_collection["crude_oil_daily_pro_change"]=df_crude_oil["pro_change"]

df_fed=pd.read_csv("../../processed/fed_rate_we_care.csv")

df_collection["fed_dff"]=df_fed["DFF"]

df_collection=df_collection.rename(columns={"tech_index_pro_change":"tech_index_daily_pro_change", "fed_dff":"fed_dff_daily"})

df_collection.to_csv("../../processed/all_data_collection.csv", index=False)

#pd05_including_monthly_values.ipynb
df_collection=pd.read_csv("../../processed/all_data_collection.csv")

df_collection["Date"]=pd.to_datetime(df_collection["Date"])

lst_nacis=["314","332","333","334","335","336","339","516","517","5132","5182"]

for nacis in lst_nacis: 
    df_input=pd.read_csv("../../processed/PPI"+nacis+"(03-25).csv")
    df_input["date"]=pd.to_datetime(df_input["date"])
    df_collection["ppi"+nacis+"_monthly_pro_change"]=df_input[df_input["date"].isin(df_collection["Date"].values)].drop_duplicates("date")["ppi_pro_change"].values*100
    
df_collection.to_csv("../../processed/all_data_collection.csv",index=False)

#pd05_considering_inflation.ipynb
df_inf_raw=pd.read_csv("../../raw/ppi_and_cpi/us_inflation_monthly.csv")
dates_month_start = pd.date_range(start='2000-01-01', end='2025-02-01', freq='MS')
df_inf_month=pd.DataFrame({"Date": dates_month_start, "cpi":df_inf_raw["Value"].values})

lst_rela=[np.nan]
for month in range(1,302):
    lst_rela.append(df_inf_month.loc[month,"cpi"]/df_inf_month.loc[month-1,"cpi"])
    
df_inf_month["rela_prior"]=lst_rela
df_inf_month["year"]=np.full(302,np.nan)
df_inf_month["month"]=np.full(302, np.nan)

for index in range(302):
    df_inf_month.loc[index,"year"]=int(df_inf_month.loc[index,"Date"].year)
    df_inf_month.loc[index,"month"]=int(df_inf_month.loc[index,"Date"].month)
    
dates=pd.date_range(start="2000-01-01",end="2025-02-28")
df_inf_day=pd.DataFrame({"Date":dates, "cpi":np.full(9191, np.nan), "rela_prior_month":np.full(9191,np.nan)})

for index in range(9191): 
    cur_year=df_inf_day.loc[index,"Date"].year
    cur_month=df_inf_day.loc[index,"Date"].month
    df_inf_day.loc[index,"cpi"]=df_inf_month[(df_inf_month["year"] == cur_year)&(df_inf_month["month"] == cur_month)]["cpi"].values[0]
    df_inf_day.loc[index,"rela_prior_month"]=df_inf_month[(df_inf_month["year"]== cur_year)&(df_inf_month["month"] == cur_month)]["rela_prior"].values[0]
    
df_20_prior=relative_period.create_rela_day_data(int_rela_day=20, str_path="../../processed/all_data_collection.csv")

df_inf_day_need=df_inf_day[df_inf_day["Date"].isin(df_20_prior["Date"].values)]
df_inf_day_need=df_inf_day_need.reset_index().drop("index",axis=1)
lst_cpi_rela=df_inf_day_need["rela_prior_month"].values
cols=df_20_prior.columns[2:]
df_20_prior_inf_adj=pd.DataFrame({"Date":df_20_prior["Date"].values, "fed_dff_daily":df_20_prior["fed_dff_daily"]})

for col in cols: 
    int_len=2568
    lst_data=df_20_prior[col].values
    lst_data=lst_data*0.01+np.ones(int_len)
    lst_data=(lst_data/lst_cpi_rela-np.ones(int_len))*100
    df_20_prior_inf_adj[col+"_inf_adj"]=lst_data
    
df_20_prior_inf_adj.to_csv("../../processed/all_data_inf_adj_collection.csv")