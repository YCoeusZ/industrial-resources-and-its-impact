# Filling in a dataframe with a col of dates linearly. This mod needs pandas. 

# If the dates are expressed as strings in form "YYYY-MM-DD" (too lazy to write for now, maybe later)

# If the dates are expressed as datetime data 

def v_datetime(df_in, str_date_col, str_col): 
    """
    Fill in data linearly
    
    :param df_in: A pandas dataframe. 
    :param str_date_col: the name of the column in df_in with dates (with type datetime). 
    :param str_col: the column whose value (floats) one wants to fill in linearly. 
    :return: the df_in but the null values of str_col are filled in linearly. 
    
    """
    lst_miss_index=df_in[df_in[str_col].isnull()].index.to_list()
    # lst_all_index=df_in.index.to_list()
    # lst_no_miss_index=[index for index in lst_all_index if index not in lst_miss_index]
    for index in lst_miss_index: 
        index_before=index
        index_after=index
        while index_before in lst_miss_index: 
            index_before-=1
        while index_after in lst_miss_index: 
            index_after+=1
        days_between=(df_in.loc[index_after][str_date_col]-df_in.loc[index_before][str_date_col]).days
        days_to=(df_in.loc[index][str_date_col]-df_in.loc[index_before][str_date_col]).days
        prop=days_to/days_between
        value_diff=df_in.loc[index_after][str_col]-df_in.loc[index_before][str_col]
        value=value_diff*prop+df_in.loc[index_before][str_col]
        df_in.at[index,str_col]=value
    return df_in