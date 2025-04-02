#This contains a function that takes all_data_collection.csv or all_data_inj_adj_collection.csv like pandas dataframe, and shift the collumn one wants to down by a number of index one wants to. 
#This function take into consideration of the missing data starting Dec-31-2016 and ending Jul-05-2017. 
#This function requires pandas and numpy 

import pandas
import numpy

def shifter(df_in, str_col, int_shift): 
    """
    A function that takes all_data_collection.csv or all_data_inj_adj_collection.csv like pandas dataframe, and shift the collumn one wants to down by a number of index one wants to. 
    This function take into consideration of the missing data starting Dec-31-2016 and ending Jul-05-2017. 
    :param df_in: A all_data_collection.csv or all_data_inj_adj_collection.csv like pandas dataframe. 
    :param str_col: The name of the column one wants to shift (down). 
    :param int_shift; number of indexes one wants to shift (down) by. 
    :return: A pandas dataframe with shifted data included. 
    """
    ind_20170706=df_in[df_in["Date"]=="2017-07-06"].index[0]
    arr_old=df_in[str_col].values
    int_len=len(arr_old)
    arr_col_new=numpy.full(int_len,numpy.nan)
    str_col_name_new=str_col+"_shifted_by_"+str(int_shift)
    for index in range(int_shift,int_len): 
        arr_col_new[index]=arr_old[index-int_shift]
    for index in range(ind_20170706,ind_20170706+int_shift): 
        arr_col_new[index]=numpy.nan 
    df_in[str_col_name_new]=arr_col_new
    
    return df_in