# This file is designed to produce the same result as the ipynb file with same name (i.e. Identify the relevant NAICS codes and produce file "com_CIK_SIC_NAICS_rele.csv"). 

import pandas as pd
import numpy as np

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