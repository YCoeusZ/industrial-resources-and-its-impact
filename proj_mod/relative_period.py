# a function that takes all_data_collection.csv to make the daily change to change relative to the data of the int_rela_day days prior. This function requires pandas imported as pandas and import numpy as numpy. 
import pandas 
import numpy 

def create_rela_day_data(int_rela_day, str_path): 
    """
        A function that takes (the path of) all_data_collection.csv to make the daily change to change relative to the data of the int_rela_day days prior. 
        This function required pandas. 
        :param int_rela_day: (Integer) The days prior one wants to relatively compare to. Good suggested values are multiples of 5, this is because there are (normaly) 5 trading days in a week, making about 20 trading days in a month. Take care not to make this value too big. 
        :param str_path: (String) Path to the all_data_collection.csv. 
        :return: (pandas.Dataframe) A dataframe changing all the daily values to change relative to the data of the int_rela_day days prior. 
    """
    df_collection=pandas.read_csv(str_path) 
    #The Date column will be remain the same 
    df_collection["Date"]=pandas.to_datetime(df_collection["Date"])
    lst_daily_val=["tech_index_daily_pro_change","cop_daily_pro_change","gold_daily_pro_change","silv_daily_pro_change","pal_daily_pro_change","plat_daily_pro_change","crude_oil_daily_pro_change"]
    int_ind_20170706=df_collection[df_collection["Date"]=="2017-07-06"].index[0]
    int_len=len(df_collection["Date"].values)
    #We go through each old column name 
    for str_col_name in lst_daily_val: 
        #Create new column name 
        str_col_new=str_col_name.replace("daily",str(int_rela_day)+"days_prior")
        #We create the new array for each column 
        arr_old=df_collection[str_col_name].values
        arr_old_rela=arr_old*0.01+numpy.ones(int_len)
        arr_new=numpy.full(int_len,numpy.nan)
        for day in range(int_rela_day,int_ind_20170706): 
            arr_new[day]=(numpy.prod(arr_old_rela[day-int_rela_day:day+1])-1)*100
        for day in range(int_ind_20170706+int_rela_day,int_len): 
            arr_new[day]=(numpy.prod(arr_old_rela[day-int_rela_day:day+1])-1)*100
        df_collection[str_col_new]=arr_new
        df_collection=df_collection.drop(str_col_name, axis=1)
    return df_collection