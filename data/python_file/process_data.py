#This file is designed to generated needed main datasets from raw. 

# import subprocess

# subprocess.call("processing/DC02/DC02_identify_relevant_NAICS.py", shell=True)
# subprocess.call("processing/DC01/DC01_collection.py", shell=True)
# subprocess.call("processing/PD01/PD01_create_weighted_index.py", shell=True)
# subprocess.call("processing/PD02/PD02_proportional_change_for_stock.py", shell=True)
# subprocess.call("processing/PD03/PD03_collection.py", shell=True)
# subprocess.call("collecting/DCO02_collecting_ppi.py", shell=True)
# subprocess.call("processing/PD04/pd04_monthly-to-daily-pro-ppi copy.py", shell=True)
# subprocess.call("processing/PD05/PD05_collection.py", shell=True)

# import sys 
# sys.path.append("../")

# from proj_mod import data_processing

#This file is created so that process_data.py can call functions from here. 

import pandas as pd
import re
import copy
import numpy as np
from datetime import datetime
import calendar
import os

import requests
import json
import prettytable

import sys
# sys.path.append("../")
import fill_in_linearly
import relative_period

def pd_dc02():
    df_com_CIK_SIC=pd.read_csv("../../raw/stock/company_CIK_SIC.csv")

    lst_sicD_maybe_tech=['Semiconductors & Related Devices', 'Electronic Connectors', 'Electronic Components, NEC', 'Electronic Components & Accessories', 'Electrical Industrial Apparatus', "Games, Toys & Children's Vehicles (No Dolls & Bicycles)", 'Telephone & Telegraph Apparatus', 'Electric Lighting & Wiring Equipment', 'Miscellaneous Electrical Machinery, Equipment & Supplies', 'Communications Equipment, NEC', 'Electronic Coils, Transformers & Other Inductors', 'Power, Distribution & Specialty Transformers', 'Measuring & Controlling Devices, NEC', 'Industrial Instruments For Measurement, Display, and Control', 'Auto Controls For Regulating Residential & Comml Environments', 'Instruments For Meas & Testing of  Electricity & Elec Signals', 'Photographic Equipment & Supplies', 'Laboratory Apparatus & Furniture', 'Electromedical & Electrotherapeutic Apparatus']

    df_com_CIK_SIC_rele=df_com_CIK_SIC.loc[(df_com_CIK_SIC.ownerOrg=="06 Technology") | (df_com_CIK_SIC.sicDescription.isin(lst_sicD_maybe_tech))]

    df_NAICS_map_SIC=pd.read_csv("../../raw/stock/tabula-NAICS-to-SIC-Crosswalk(2017-2022).csv")

    dict_SIC_NAICS={}
    for ind in range(2105):
        dict_SIC_NAICS[df_NAICS_map_SIC.loc[ind,"SIC"]]=set(df_NAICS_map_SIC.loc[df_NAICS_map_SIC.SIC==df_NAICS_map_SIC.loc[ind,"SIC"]].NAICS)
    
    lst_SIC_no_NAICS=[]

    for sic in df_com_CIK_SIC.sic: 
        if not np.isnan(sic):
            if str(int(sic)) not in dict_SIC_NAICS: 
                lst_SIC_no_NAICS.append(sic)
            
    lst_SIC_no_NAICS_rele=[]

    for sic in df_com_CIK_SIC_rele.sic: 
        if not np.isnan(sic):
            if str(int(sic)) not in dict_SIC_NAICS: 
                lst_SIC_no_NAICS_rele.append(sic)
            
    dict_append={"3620": {335999}, "3550":{332410, 333111, 333242, 333249, 333318}, "3590":{332710, 332813, 332999, 333318, 333999, 336390}, "3560":{314999, 333414, 333999}, "7370":{518210, 541511}, "3530":{333131}, "3640":{335121, 335122}, "3690":{333318, 333618, 333992, 335129, 335999}, "3570":{334111, 334112},"3540":{333519},"3510":{333611, 333618, 336390}, "3670":{333318, 334519, 339940},"3576":{333316, 333318, 334111, 334112, 334118},"3580":{333415},"3678":{334220, 334310, 334418, 334419, 334515}}

    dict_SIC_NAICS.update(dict_append)

    lst_sic_rele=list(df_com_CIK_SIC_rele.sic)

    lst_naics_rele=[]

    for ele in lst_sic_rele: 
        lst_naics_rele.append(dict_SIC_NAICS[str(int(ele))])
    
    # lst_naics_rele

    lst_str_naics_rele=[",".join(str(naics) for naics in ele) for ele in lst_naics_rele]

    df_com_CIK_SIC_rele["naics"]=lst_str_naics_rele

    df_com_CIK_SIC_rele.to_csv("../../processed/com_CIK_SIC_NAICS_rele.csv", index=False)
    
def pd_dc01():
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

    for lst_ele in lst_gpt_gen_new_old_tic: 
        if lst_ele[1]=="None": 
            print(lst_ele, lst_gpt_gen_new_old_tic.index(lst_ele))
            
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
    def tic_is_in(str_tic): 
        if str_tic in lst_his_weight_tic_wrd: 
            com_tic=re.findall("([A-Za-z0-9]*)[^A-Za-z0-9]",str_tic)[0]
            return com_tic in lst_tech_sp500_tic_sing
        else: 
            return str_tic in lst_tech_sp500_tic_sing 

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

def pd_pd01():
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

def pd_pd02():
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

def pd_pd03():

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

def process_ppi_files(filepaths):
    """
    Reads multiple text files that each contain columns like:
        series id | year | period | value | footnotes
    and combines them into a single DataFrame covering 2003–2025.
    
    Steps:
      1) Remove table border lines (lines starting with '+').
      2) Parse the first non-removed line as the header.
      3) Convert 'year' and 'value' to numeric; extract 'month' from 'period'.
      4) Rename 'value' to 'ppi'.
      5) Sort by (year, month) across *all* combined data.
      6) Compute month-over-month proportional change (ppi_pro_change) across the entire timespan.
      7) Expand monthly rows into daily rows.
      8) Extract the NACIS code from the first file's name (or verify all have same NACIS).
      9) Save the final daily DataFrame to 'data/processed/PPI{nacis}(03-25).csv'.
      
    Returns:
      The combined daily DataFrame (covering 03-25).
    """
    
    
    all_dfs = []
    
    for filepath in filepaths:
        # Read and parse each file individually
        
        with open(filepath, 'r') as f:
            lines = f.readlines()
        
        # Remove lines that start with '+'
        data_lines = [line for line in lines if not line.startswith('+')]
        
        data_clean = []
        for line in data_lines:
            line_strip = line.strip()
            if not line_strip:
                continue
            parts = [x.strip() for x in line_strip.strip('|').split('|')]
            data_clean.append(parts)
        
        header = data_clean[0]
        rows = data_clean[1:]
        df_temp = pd.DataFrame(rows, columns=header)
        
        # Check columns
        needed_cols = {'year', 'period', 'value'}
        if not needed_cols.issubset(df_temp.columns):
            print(f"ERROR: Missing columns in {filepath}. Found: {df_temp.columns.tolist()}")
            continue
        
        # Convert columns
        df_temp['year'] = df_temp['year'].astype(int)
        df_temp['value'] = df_temp['value'].astype(float)
        
        # Extract month
        df_temp['month'] = df_temp['period'].str.extract(r'M(\d+)').astype(int)
        
        # Rename 'value' -> 'ppi'
        df_temp.rename(columns={'value': 'ppi'}, inplace=True)
        
        # Keep only relevant columns
        df_temp = df_temp[['year', 'month', 'ppi']]
        
        all_dfs.append(df_temp)
    
    
    combined_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(["year","month"],keep="first")
    combined_df.sort_values(by=['year', 'month'], inplace=True)
    

    combined_df['ppi_pro_change'] = combined_df['ppi'].pct_change()
    
    # Now we expand monthly data to daily
    daily_rows = []
    for _, row in combined_df.iterrows():
        year = int(row['year'])
        month = int(row['month'])
        days_in_month = calendar.monthrange(year, month)[1]
        start_date = datetime(year, month, 1)
        
        date_range = pd.date_range(start=start_date, periods=days_in_month)
        for single_day in date_range:
            daily_rows.append({
                'date': single_day,
                'ppi': row['ppi'],
                'ppi_pro_change': row['ppi_pro_change']
            })
    
    daily_df = pd.DataFrame(daily_rows)
    
    # Extract NACIS code from the *first* file in filepaths
    basename = os.path.basename(filepaths[0])
    match = re.search(r'PCU(\d+)', basename)
    nacis = match.group(1) if match else "unknown"
        
    out_dir = "../../processed"
    os.makedirs(out_dir, exist_ok=True)
    out_filename = f"PPI{nacis}(03-25).csv"
    out_path = os.path.join(out_dir, out_filename)
    
    daily_df.to_csv(out_path, index=False)

    
    return daily_df


def lst_set_union(lst_set): 
    union_set=set()
    for ele_set in lst_set: 
        union_set=union_set.union(ele_set)
    return union_set

def pd_dco02():
    df_tech_500=pd.read_csv("../processed/tech_sp500_stable_extended_tickers.csv")
    lst_naics_str=list(df_tech_500.naics)
    lst_naics_set=[set(ele_str.split(",")) for ele_str in lst_naics_str]
    set_naics=lst_set_union(lst_naics_set)

    set_naics_short=set()

    for ele_str in set_naics: 
        set_naics_short.add(ele_str[:3]+"---")
        
    df_naics_ppi=pd.read_table("../raw/ppi_and_cpi/ppi_ind_series.txt")

    set_naics_short=set_naics_short.union({"5415--","5182--","5151--","5152--","5112--"})

    set_naics_short=set_naics_short.union({"516---","5132--"})

    df_ppi_care=df_naics_ppi[df_naics_ppi["industry_code"].isin(set_naics_short)]

    series_care=list(df_ppi_care["series_id                     "])

    series_care=[ele_str[:15] for ele_str in series_care]

    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": series_care,"startyear":"2003", "endyear":"2012"})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    json_data = json.loads(p.text)
    for series in json_data['Results']['series']:
        x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            footnotes=""
            for footnote in item['footnotes']:
                if footnote:
                    footnotes = footnotes + footnote['text'] + ','
            if 'M01' <= period <= 'M12':
                x.add_row([seriesId,year,period,value,footnotes[0:-1]])
        output = open("../raw/ppi_and_cpi/"+seriesId + '(03-12).txt','w')
        output.write (x.get_string())
        output.close()
        
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": series_care,"startyear":"2012", "endyear":"2020"})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    json_data = json.loads(p.text)
    for series in json_data['Results']['series']:
        x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            footnotes=""
            for footnote in item['footnotes']:
                if footnote:
                    footnotes = footnotes + footnote['text'] + ','
            if 'M01' <= period <= 'M12':
                x.add_row([seriesId,year,period,value,footnotes[0:-1]])
        output = open("../raw/ppi_and_cpi/"+seriesId + '(12-20).txt','w')
        output.write (x.get_string())
        output.close()
        
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": series_care,"startyear":"2020", "endyear":"2025"})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    json_data = json.loads(p.text)
    for series in json_data['Results']['series']:
        x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            footnotes=""
            for footnote in item['footnotes']:
                if footnote:
                    footnotes = footnotes + footnote['text'] + ','
            if 'M01' <= period <= 'M12':
                x.add_row([seriesId,year,period,value,footnotes[0:-1]])
        output = open("../raw/ppi_and_cpi/"+seriesId + '(20-25).txt','w')
        output.write (x.get_string())
        output.close()

def pd_pd04():
    lst_314=["../../raw/ppi_and_cpi/PCU314---314---(03-12).txt",
        "../../raw/ppi_and_cpi/PCU314---314---(12-20).txt",
        "../../raw/ppi_and_cpi/PCU314---314---(20-25).txt"]
    lst_332=["../../raw/ppi_and_cpi/PCU332---332---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU332---332---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU332---332---(20-25).txt"]
    lst_333=["../../raw/ppi_and_cpi/PCU333---333---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU333---333---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU333---333---(20-25).txt"]
    lst_334=["../../raw/ppi_and_cpi/PCU334---334---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU334---334---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU334---334---(20-25).txt"]
    lst_335=["../../raw/ppi_and_cpi/PCU335---335---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU335---335---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU335---335---(20-25).txt"]
    lst_336=["../../raw/ppi_and_cpi/PCU336---336---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU336---336---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU336---336---(20-25).txt"]
    lst_339=["../../raw/ppi_and_cpi/PCU339---339---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU339---339---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU339---339---(20-25).txt"]
    lst_516=["../../raw/ppi_and_cpi/PCU516---516---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU516---516---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU516---516---(20-25).txt"]
    lst_517=["../../raw/ppi_and_cpi/PCU517---517---(03-12).txt",
            "../../raw/ppi_and_cpi/PCU517---517---(12-20).txt",
            "../../raw/ppi_and_cpi/PCU517---517---(20-25).txt"]
    lst_5132=["../../raw/ppi_and_cpi/PCU5132--5132--(03-12).txt",
            "../../raw/ppi_and_cpi/PCU5132--5132--(12-20).txt",
            "../../raw/ppi_and_cpi/PCU5132--5132--(20-25).txt"]
    lst_5182=["../../raw/ppi_and_cpi/PCU5182--5182--(03-12).txt",
            "../../raw/ppi_and_cpi/PCU5182--5182--(12-20).txt",
            "../../raw/ppi_and_cpi/PCU5182--5182--(20-25).txt"]

    lst_of_nacis=[lst_314,lst_332,lst_335,lst_333,lst_334,lst_336,lst_339,lst_516,lst_517,lst_5132,lst_5182]

    for nacis in lst_of_nacis: 
        process_ppi_files(nacis)
        
def pd_pd05():
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

# data_processing.pd_dc02()
# data_processing.pd_dc01()
# data_processing.pd_pd01()
# data_processing.pd_pd02()
# data_processing.pd_pd03()
# data_processing.pd_dco02()
# data_processing.pd_pd04()
# data_processing.pd_pd05()


