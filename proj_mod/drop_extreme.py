#This is a function that takes a pandas dataframe and drops the extreme values (quantiles below fl_low and above fl_high) for each column. This function requires pandas. 
import pandas 

def drop_quantile(fl_low,fl_high,df_in): 
    """
    This is a function that takes a pandas dataframe and drops the extreme values (quantiles below fl_low and above fl_high) for each column. This function requires pandas. 
    :param fl_low: The lower quantile to be droped. 
    :param fl_high: The higher quantile to be droped. 
    :param df_in: The dataframe to be operated on. 
    :return: A date frame with extreme values droped in each column. 
    """
    df_in_c=df_in.copy(deep=True)
    lst_col=df_in_c.columns 
    for col in lst_col: 
        v_low=df_in_c[col].quantile(fl_low, interpolation="nearest")
        v_high=df_in_c[col].quantile(fl_high, interpolation="nearest")
        df_in_c=df_in_c[(df_in_c[col]<v_high)&(df_in_c[col]>v_low)]
    return df_in_c

def drop_by_sort(fl_low,fl_high,df_in): 
    """
    This is a function that takes a pandas dataframe and drops the extreme values (quantiles below fl_low and above fl_high) for each column. This function requires pandas. This function does a very stupid sort on each column, so it might take a while to run. 
    :param fl_low: The lower quantile to be droped. 
    :param fl_high: The higher quantile to be droped. 
    :param df_in: The dataframe to be operated on. 
    :return: A date frame with extreme values droped in each column. 
    """
    df_in_c=df_in.copy(deep=True)
    lst_col=df_in_c.columns 
    for col in lst_col: 
        lst_val=list(df_in_c[col].values)
        lst_val.sort()
        int_len=len(lst_val)
        ind_low=int(int_len*fl_low)
        ind_high=int(int_len*fl_high)
        v_low=lst_val[ind_low]
        v_high=lst_val[ind_high]
        df_in_c=df_in_c[(df_in_c[col]<=v_high)&(df_in_c[col]>=v_low)]
    return df_in_c
