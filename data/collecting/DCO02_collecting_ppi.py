# This files is designed so that it perform the same task as the ipynb document of same name (i.e. collect PPI based on selected NAICS). 

import pandas as pd

import requests
import json
import prettytable

def lst_set_union(lst_set): 
    union_set=set()
    for ele_set in lst_set: 
        union_set=union_set.union(ele_set)
    return union_set

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