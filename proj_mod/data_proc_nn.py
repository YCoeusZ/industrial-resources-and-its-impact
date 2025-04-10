# Following function requires numpy as np and pandas as pd. 
import pandas as pd
import numpy as np 

def create_data_RNN(df_X_in,df_y_in,int_lag,int_jump=1): 
    """
    :param df_X_in: A dataframe of training parameters. 
    :param df_y_in: A dataframe of training target with only one column and same number of rows as df_X_in. 
    :param int_lag: How many historical value, in addition to current parameters, to include. 
    "param int_jump: The jump between two historical values. 
    :return: arr_X then arr_y as the training data. 
    """
    df_X_in_c=df_X_in.copy(deep=True).reset_index(drop=True)
    df_y_in_c=df_y_in.copy(deep=True).reset_index(drop=True)
    int_len=len(df_X_in_c)
    int_len_X_col=len(df_X_in_c.columns)
    lst_X=np.zeros(shape=(int_len-int_lag,int_lag+1,int_len_X_col))
    lst_y=df_y_in_c.iloc[int_lag:,0].values
    for index in range(int_lag,int_len): 
        for sub_ind in range(0,int_lag+1): 
            lst_X[index-int_lag, int_lag-sub_ind]= df_X_in_c.iloc[index-int_jump*sub_ind,:].values
    return lst_X, lst_y