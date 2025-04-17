# This file is designed to produce the same result as the ipynb file with the same name (i.e. processing monthly data, and produce files "PPI*****.csv")

import pandas as pd
import numpy as np
from datetime import datetime
import calendar
import os
import re

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
    
