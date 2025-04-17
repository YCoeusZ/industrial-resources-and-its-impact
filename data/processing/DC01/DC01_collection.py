# This file is designed to produce the same results as "DC01_finding_techs_in_sp500.ipynb", "DC01_finding_stable_com.ipynb", and "DC01_finding_stable_14_24.ipynb" combined (i.e. identify stable tech companies, and produce files "../../processed/his_weight_fix_tic_in_process.csv", "../../processed/com_CIK_SIC_NAICS_rele.csv", "../../processed/tech_sp500_com_CIK_SIC_NAICS.csv", "../../processed/his_weight_12_24_in_progress.csv", "../../processed/his_weight_tech_500_12_24_in_progress.csv", "../../processed/tech_sp500_stable.csv", "../../processed/tech_sp500_stable_extended_tickers.csv", and "../../processed/tech_sp500_stable_extended_tickers.csv"). 

import pandas as pd
import re
import copy

#DC01_finding_techs_in_sp500.ipynb
df_his_weight=pd.read_csv("../../raw/stock/sp500_historical_weight.csv")
df_com_CIK_SIC_NAICS_rele=pd.read_csv("../../processed/com_CIK_SIC_NAICS_rele.csv")

lst_weight_ticker=list(df_his_weight.ticker.unique())

reg_allow=re.compile("^[A-Za-z0-9]*$")
lst_weight_ticker_weird=[tic for tic in lst_weight_ticker if not bool(reg_allow.fullmatch(tic))]

df_his_weight.at[543629,"ticker"]="XL"

df_his_weight.at[899332,"ticker"]="DISH"
df_his_weight.at[899838,"ticker"]="DISH"
df_his_weight.at[900344,"ticker"]="DISH"

df_his_weight.at[943916,"ticker"]="AON"

df_his_weight.at[976618,"ticker"]="OXY"
df_his_weight.at[977125,"ticker"]="OXY"
df_his_weight.at[977632,"ticker"]="OXY"
df_his_weight.at[978139,"ticker"]="OXY"
df_his_weight.at[978646,"ticker"]="OXY"
df_his_weight.at[979153,"ticker"]="OXY"
df_his_weight.at[979660,"ticker"]="OXY"
df_his_weight.at[980167,"ticker"]="OXY"
df_his_weight.at[980674,"ticker"]="OXY"
df_his_weight.at[981181,"ticker"]="OXY"
df_his_weight.at[981688,"ticker"]="OXY"

#backup
df_his_weight.to_csv("../../processed/his_weight_fix_tic_in_process.csv", index=False)

df_his_weight=df_his_weight[df_his_weight.ticker!="-"]

#backup
df_his_weight.to_csv("../../processed/his_weight_fix_tic_in_process.csv", index=False)

lst_weight_ticker=list(df_his_weight.ticker.unique())

reg_allow=re.compile("^[A-Za-z0-9]*$")
lst_weight_ticker_weird=[tic for tic in lst_weight_ticker if not bool(reg_allow.fullmatch(tic))]

lst_tech_adj_ticker = list(df_com_CIK_SIC_NAICS_rele.tickers.unique())

df_com_CIK_SIC_NAICS_rele.loc[445,"tickers"]="WKME"
df_com_CIK_SIC_NAICS_rele.loc[872,"tickers"]="MRDB"
df_com_CIK_SIC_NAICS_rele.loc[1518,"tickers"]="MRDB"
df_com_CIK_SIC_NAICS_rele.loc[949,"tickers"]="NA"
df_com_CIK_SIC_NAICS_rele.loc[1165,"tickers"]="TWOUQ"
df_com_CIK_SIC_NAICS_rele.loc[1213,"tickers"]="SSOF"
df_com_CIK_SIC_NAICS_rele.loc[1221,"tickers"]="EBIXQ"
df_com_CIK_SIC_NAICS_rele.loc[1227,"tickers"]="GMHS"
df_com_CIK_SIC_NAICS_rele.loc[1228,"tickers"]="NTCL"
df_com_CIK_SIC_NAICS_rele.loc[1229,"tickers"]="HAHA"
df_com_CIK_SIC_NAICS_rele.loc[1231,"tickers"]="WRD"
df_com_CIK_SIC_NAICS_rele.loc[1236,"tickers"]="UEOP"
df_com_CIK_SIC_NAICS_rele.loc[1242,"tickers"]="SSAI"
df_com_CIK_SIC_NAICS_rele.loc[1308,"tickers"]="SFUNY"
df_com_CIK_SIC_NAICS_rele.loc[1319,"tickers"]="WHSI"
df_com_CIK_SIC_NAICS_rele.loc[1385,"tickers"]="DPSI"
df_com_CIK_SIC_NAICS_rele.loc[1505,"tickers"]="LKRY"

df_com_CIK_SIC_NAICS_rele.to_csv("../../processed/com_CIK_SIC_NAICS_rele.csv",index=False)

lst_tech_adj_ticker = list(df_com_CIK_SIC_NAICS_rele.tickers.unique())
lst_tech_adj_ticker_SINGULAR = []

for i in range(len(lst_tech_adj_ticker)): 
    lst_tech_adj_ticker_SINGULAR = lst_tech_adj_ticker_SINGULAR + lst_tech_adj_ticker[i].split(",")
    
lst_tech_adj_ticker_SINGULAR_weird=[tic for tic in lst_tech_adj_ticker_SINGULAR if not bool(reg_allow.fullmatch(tic))]

set_tech_adj_ticker_wrd_symb=set()

for tic in lst_tech_adj_ticker_SINGULAR_weird: 
    # print(tic)
    wrd_symb=re.findall("[A-Za-z0-9]*([^A-Za-z0-9])",tic)[0]
    set_tech_adj_ticker_wrd_symb.add(wrd_symb)
    
for tic in lst_tech_adj_ticker_SINGULAR_weird: 
    com_tic=re.findall("([A-Za-z0-9]*)\-",tic)[0]
    if com_tic not in lst_tech_adj_ticker_SINGULAR: 
        lst_tech_adj_ticker_SINGULAR.append(com_tic)
        
for tic in lst_weight_ticker_weird: 
    # print(tic)
    com_tic=re.findall("([A-Za-z0-9]*)[^A-Za-z0-9]",tic)[0]
    if com_tic not in lst_weight_ticker: 
        lst_weight_ticker.append(com_tic)
        
set_weight_ticker_wrd_symb=set()

for tic in lst_weight_ticker_weird: 
    # print(tic)
    wrd_symb=re.findall("[A-Za-z0-9]*([^A-Za-z0-9])",tic)[0]
    set_weight_ticker_wrd_symb.add(wrd_symb)
    
lst_weight_ticker_changed=copy.deepcopy(lst_weight_ticker)

for tic in lst_weight_ticker_changed: 
    for symb in set_weight_ticker_wrd_symb: 
        tic.replace(symb,"-")
        
set_tech_sp_ticker=set(lst_tech_adj_ticker_SINGULAR) & set(lst_weight_ticker_changed)

set_tech_sp_wrd_symb=set()

for tic in set_tech_sp_ticker: 
    # print(tic)
    if re.findall("[A-Za-z0-9]*([^A-Za-z0-9])",tic):
        wrd_symb=re.findall("[A-Za-z0-9]*([^A-Za-z0-9])",tic)[0]
        set_tech_sp_wrd_symb.add(wrd_symb)
        
lst_tickers_keep=[]

for tickers in lst_tech_adj_ticker: 
    for tick in tickers.split(","): 
        lst_tick_proc=re.findall("([A-Za-z0-9]*)[^A-Za-z0-9]",tick)
        if len(lst_tick_proc) == 0: 
            if tick in set_tech_sp_ticker:
                lst_tickers_keep.append(tickers)
        else: 
            if lst_tick_proc[0] in set_tech_sp_ticker: 
                lst_tickers_keep.append(tickers)
                
df_tech_sp_com_CIK_SIC_NAICS=df_com_CIK_SIC_NAICS_rele[df_com_CIK_SIC_NAICS_rele.tickers.isin(lst_tickers_keep)].drop_duplicates()

df_tech_sp_com_CIK_SIC_NAICS.to_csv("../../processed/tech_sp500_com_CIK_SIC_NAICS.csv",index=False)

#DC01_finding_stable_com.ipynb
df_tech_sp500=pd.read_csv("../../processed/tech_sp500_com_CIK_SIC_NAICS.csv")

lst_gpt_gen_new_old_tic = [
    ["Apple Inc.", "AAPL", "None"],
    ["MICROSOFT CORP", "MSFT", "None"],
    ["NVIDIA CORP", "NVDA", "None"],
    ["Alphabet Inc.", "GOOGL/GOOG", "None"],
    ["Meta Platforms, Inc.", "META", "FB"],
    ["Broadcom Inc.", "AVGO", "BRCM"],
    ["ORACLE CORP", "ORCL", "None"],
    ["Salesforce, Inc.", "CRM", "None"],
    ["ADOBE INC.", "ADBE", "None"],
    ["ADVANCED MICRO DEVICES INC", "AMD", "None"],
    ["THERMO FISHER SCIENTIFIC INC.", "TMO", "None"],
    ["T-Mobile US, Inc.", "TMUS", "None"],
    ["CISCO SYSTEMS, INC.", "CSCO", "None"],
    ["DANAHER CORP /DE/", "DHR", "None"],
    ["INTUIT INC.", "INTU", "None"],
    ["TEXAS INSTRUMENTS INC", "TXN", "None"],
    ["INTERNATIONAL BUSINESS MACHINES CORP", "IBM", "None"],
    ["APPLIED MATERIALS INC /DE", "AMAT", "None"],
    ["ServiceNow, Inc.", "NOW", "None"],
    ["VERIZON COMMUNICATIONS INC", "VZ", "None"],
    ["CATERPILLAR INC", "CAT", "None"],
    ["COMCAST CORP", "CMCSA", "None"],
    ["AT&T INC.", "T", "None"],
    ["MICRON TECHNOLOGY INC", "MU", "None"],
    ["Eaton Corp plc", "ETN", "None"],
    ["LAM RESEARCH CORP", "LRCX", "None"],
    ["Arista Networks, Inc.", "ANET", "None"],
    ["ANALOG DEVICES INC", "ADI", "None"],
    ["Palo Alto Networks Inc", "PANW", "None"],
    ["Medtronic plc", "MDT", "None"],
    ["AUTOMATIC DATA PROCESSING INC", "ADP", "None"],
    ["DEERE & CO", "DE", "None"],
    ["INTEL CORP", "INTC", "None"],
    ["SYNOPSYS INC", "SNPS", "None"],
    ["AMPHENOL CORP /DE/", "APH", "None"],
    ["Dell Technologies Inc.", "DELL", "None"],
    ["CADENCE DESIGN SYSTEMS INC", "CDNS", "None"],
    ["Trane Technologies plc", "TT", "None"],
    ["Palantir Technologies Inc.", "PLTR", "None"],
    ["ILLINOIS TOOL WORKS INC", "ITW", "None"],
    ["NXP Semiconductors N.V.", "NXPI", "None"],
    ["CrowdStrike Holdings, Inc.", "CRWD", "None"],
    ["CARRIER GLOBAL Corp", "CARR", "None"],
    ["ROPER TECHNOLOGIES INC", "ROP", "None"],
    ["Fortinet, Inc.", "FTNT", "None"],
    ["Autodesk, Inc.", "ADSK", "None"],
    ["CHARTER COMMUNICATIONS, INC. /MO/", "CHTR", "None"],
    ["Johnson Controls International plc", "JCI", "None"],
    ["MONOLITHIC POWER SYSTEMS INC", "MPWR", "None"],
    ["MICROCHIP TECHNOLOGY INC", "MCHP", "None"],
    ["CUMMINS INC", "CMI", "None"],
    ["ELECTRONIC ARTS INC.", "EA", "None"],
    ["Verisk Analytics, Inc.", "VRSK", "None"],
    ["AMETEK INC/", "AME", "None"],
    ["COGNIZANT TECHNOLOGY SOLUTIONS CORP", "CTSH", "None"],
    ["Ingersoll Rand Inc.", "IR", "None"],
    ["Super Micro Computer, Inc.", "SMCI", "None"],
    ["HP INC", "HPQ", "None"],
    ["Baker Hughes Co", "BKR", "None"],
    ["ON SEMICONDUCTOR CORP", "ON", "None"],
    ["Xylem Inc.", "XYL", "None"],
    ["ROCKWELL AUTOMATION, INC", "ROK", "None"],
    ["ANSYS INC", "ANSS", "None"],
    ["NetApp, Inc.", "NTAP", "None"],
    ["Veralto Corp", "VRTX", "None"],
    ["TAKE TWO INTERACTIVE SOFTWARE INC", "TTWO", "None"],
    ["TYLER TECHNOLOGIES INC", "TYL", "None"],
    ["DOVER Corp", "DOV", "None"],
    ["Hewlett Packard Enterprise Co", "HPE", "None"],
    ["Fortive Corp", "FTV", "None"],
    ["FIRST SOLAR, INC.", "FSLR", "None"],
    ["Keysight Technologies, Inc.", "KEYS", "None"],
    ["GoDaddy Inc.", "GDDY", "None"],
    ["TERADYNE, INC", "TER", "None"],
    ["Seagate Technology Holdings plc", "STX", "None"],
    ["WESTERN DIGITAL CORP", "WDC", "None"],
    ["PTC INC.", "PTC", "None"],
    ["HUBBELL INC", "HUBB", "None"],
    ["Leidos Holdings, Inc.", "LDOS", "None"],
    ["Warner Bros. Discovery, Inc.", "WBD", "None"],
    ["ZEBRA TECHNOLOGIES CORP", "ZBRA", "None"],
    ["Fox Corp", "FOXA/FOXA", "None"],
    ["VERISIGN INC/CA", "VRSN", "None"],
    ["SKYWORKS SOLUTIONS, INC.", "SWKS", "None"],
    ["Enphase Energy, Inc.", "ENPH", "None"],
    ["FACTSET RESEARCH SYSTEMS INC", "FDS", "None"],
    ["Gen Digital Inc.", "GEN", "None"],
    ["IDEX CORP /DE/", "IEX", "None"],
    ["PENTAIR plc", "PNR", "None"],
    ["NORDSON CORP", "NDSN", "None"],
    ["TRIMBLE INC.", "TRMB", "None"],
    ["JUNIPER NETWORKS INC", "JNPR", "None"],
    ["JACK HENRY & ASSOCIATES INC", "JKHY", "None"],
    ["TechnipFMC plc", "FTI", "None"],
    ["EPAM Systems, Inc.", "EPAM", "None"],
    ["F5, INC.", "FFIV", "None"],
    ["ITT INC.", "ITT", "None"],
    ["Qorvo, Inc.", "QRVO", "None"],
    ["HASBRO, INC.", "HAS", "None"],
    ["Paycom Software, Inc.", "PAYC", "None"],
    ["Match Group, Inc.", "MTCH", "None"],
    ["Dayforce, Inc.", "None", "None"],
    ["Paramount Global", "PARA", "None"],
    ["CIENA CORP", "CIEN", "None"],
    ["Altair Engineering Inc.", "ALTR", "None"],
    ["ACUITY BRANDS INC", "AYI", "None"],
    ["SentinelOne, Inc.", "S", "None"],
    ["NOV Inc.", "NOV", "None"],
    ["HashiCorp, Inc.", "HCP", "None"],
    ["FLOWSERVE CORP", "FLS", "None"],
    ["Lumen Technologies, Inc.", "LUMN", "None"],
    ["DXC Technology Co", "DXC", "None"],
    ["DoubleVerify Holdings, Inc.", "DV", "None"],
    ["IPG PHOTONICS CORP", "IPGP", "None"],
    ["TERADATA CORP /DE/", "TDC", "None"],
    ["TEGNA INC", "TGNA", "None"],
    ["TripAdvisor, Inc.", "TRIP", "None"],
    ["SOLAREDGE TECHNOLOGIES, INC.", "SEDG", "None"],
    ["DNOW Inc.", "DNOW", "None"],
    ["PITNEY BOWES INC /DE/", "PBI", "None"],
    ["Xerox Holdings Corp", "XRX", "None"],
    ["MANITOWOC CO INC", "MTW", "None"],
    ["Solidion Technology Inc.", "None", "None"],
    ["Super League Enterprise, Inc.", "None", "None"]
]

# for lst_ele in lst_gpt_gen_new_old_tic: 
#     if lst_ele[1]=="None": 
#         print(lst_ele, lst_gpt_gen_new_old_tic.index(lst_ele))
        
for lst_ele in lst_gpt_gen_new_old_tic: 
    if lst_ele[1]=="None":
        lst_gpt_gen_new_old_tic[lst_gpt_gen_new_old_tic.index(lst_ele)][1]=df_tech_sp500[df_tech_sp500.name==lst_ele[0]].tickers.unique()[0]
        
df_tech_sp500.at[4,"tickers"]=df_tech_sp500.at[4,"tickers"]+",FB"

df_tech_sp500.at[5,"tickers"]=df_tech_sp500.at[5,"tickers"]+",BRCM"

lst_tech_sp500_tic=df_tech_sp500.tickers.to_list()

lst_tech_sp500_tic_sing=[]

for str_ele in lst_tech_sp500_tic: 
    lst_tech_sp500_tic_sing=lst_tech_sp500_tic_sing+str_ele.split(",")
    
# reg_allow=re.compile("^[A-Za-z0-9]*$")
# lst_tech_sp500_tic_sing_weird=[tic for tic in lst_tech_sp500_tic_sing if not bool(reg_allow.fullmatch(tic))]

df_his_weight=pd.read_csv("../../processed/his_weight_fix_tic_in_process.csv")

df_his_weight_12_24=df_his_weight[df_his_weight.date >= "2012-04-30"]

df_his_weight_12_24.to_csv("../../processed/his_weight_12_24_in_progress.csv", index=False)

df_his_weight_12_24=pd.read_csv("../../processed/his_weight_12_24_in_progress.csv")

lst_his_weight_tic=list(df_his_weight_12_24.ticker.unique())

lst_his_weight_tic_wrd=[tic for tic in lst_his_weight_tic if not bool(reg_allow.fullmatch(tic))]

def tic_is_in(str_tic): 
    if str_tic in lst_his_weight_tic_wrd: 
        com_tic=re.findall("([A-Za-z0-9]*)[^A-Za-z0-9]",str_tic)[0]
        return com_tic in lst_tech_sp500_tic_sing
    else: 
        return str_tic in lst_tech_sp500_tic_sing 
    
lst_weight_tic_in_tech_500=[str_tic for str_tic in lst_his_weight_tic if tic_is_in(str_tic)]

df_his_weight_tech_500_12_24=df_his_weight_12_24[df_his_weight_12_24.ticker.isin(lst_weight_tic_in_tech_500)]

df_his_weight_tech_500_12_24.to_csv("../../processed/his_weight_tech_500_12_24_in_progress.csv",index=False)

df_his_weight_tech_500_12_24=pd.read_csv("../../processed/his_weight_tech_500_12_24_in_progress.csv")

lst_tech_sp500_tic_set=[set(str_tics.split(",")) for str_tics in lst_tech_sp500_tic]

for set_ele in lst_tech_sp500_tic_set: 
    for str_tic in lst_his_weight_tic_wrd: 
        if (re.findall("([A-Za-z0-9]*)[^A-Za-z0-9]",str_tic)[0] in set_ele) and (str_tic not in set_ele): 
            set_ele.add(str_tic)
            
lst_stable=[]

for ele_set in lst_tech_sp500_tic_set: 
    lst_stable.append(len(df_his_weight_tech_500_12_24[df_his_weight_tech_500_12_24.ticker.isin(ele_set)])>=2990)
    
lst_stable_count=[]

for ele_set in lst_tech_sp500_tic_set: 
    lst_stable_count.append(len(df_his_weight_tech_500_12_24[df_his_weight_tech_500_12_24.ticker.isin(ele_set)]))
    
df_tech_sp500["stable"]=lst_stable

df_tech_sp500.rename(columns={"stable":"stable 12 to 24"})

df_tech_sp500.to_csv("../../processed/tech_sp500_stable.csv", index=False)

#Further needed created file for extended tickers 
df_his_weight=pd.read_csv("../../processed/his_weight_tech_500_12_24_in_progress.csv")
df_tech_sp500=pd.read_csv("../../processed/tech_sp500_stable.csv")
lst_tech_sp500_tic=df_tech_sp500.tickers.to_list()
lst_tech_sp500_tic_set=[set(str_tics.split(",")) for str_tics in lst_tech_sp500_tic]
lst_tech_sp500_tic_sing=[]

for str_ele in lst_tech_sp500_tic: 
    lst_tech_sp500_tic_sing=lst_tech_sp500_tic_sing+str_ele.split(",")
    
lst_his_weight_tic=list(df_his_weight.ticker.unique())
reg_allow=re.compile("^[A-Za-z0-9]*$")
lst_his_weight_tic_wrd=[tic for tic in lst_his_weight_tic if not bool(reg_allow.fullmatch(tic))]

# def tic_is_in(str_tic): 
#     if str_tic in lst_his_weight_tic_wrd: 
#         com_tic=re.findall("([A-Za-z0-9]*)[^A-Za-z0-9]",str_tic)[0]
#         return com_tic in lst_tech_sp500_tic_sing
#     else: 
#         return str_tic in lst_tech_sp500_tic_sing 

lst_weight_tic_in_tech_500=[str_tic for str_tic in lst_his_weight_tic if tic_is_in(str_tic)]

for set_ele in lst_tech_sp500_tic_set: 
    for str_tic in lst_his_weight_tic_wrd: 
        if (re.findall("([A-Za-z0-9]*)[^A-Za-z0-9]",str_tic)[0] in set_ele) and (str_tic not in set_ele): 
            set_ele.add(str_tic)
            
lst_tech_sp500_tic_extend=[]

for set_ele in lst_tech_sp500_tic_set: 
    lst_tech_sp500_tic_extend.append(",".join(set_ele))
    
df_tech_sp500["extended tickers"]=lst_tech_sp500_tic_extend

df_tech_sp500.to_csv("../../processed/tech_sp500_stable_extended_tickers.csv", index=False)

#DC01_finding_stable_14_24.ipynb
df_tech_sp500=pd.read_csv("../../processed/tech_sp500_stable_extended_tickers.csv")
df_his_weight=pd.read_csv("../../processed/his_weight_tech_500_12_24_in_progress.csv")
df_his_weight_14_24=df_his_weight[df_his_weight.date>"2013-12-31"]
lst_tics_tech_500_set=[set(str_ele.split(",")) for str_ele in list(df_tech_sp500["extended tickers"])]
lst_stable_14_24=[]

for ele_set in lst_tics_tech_500_set: 
    lst_stable_14_24.append(len(df_his_weight_14_24[df_his_weight_14_24.ticker.isin(ele_set)])>=2570)

df_tech_sp500["stable 14 to 24"]=lst_stable_14_24
df_tech_sp500.to_csv("../../processed/tech_sp500_stable_extended_tickers.csv")