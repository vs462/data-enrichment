import pandas as pd
import os
import glob
#import psycopg2
from secrete import conn_string_secrete

""" 
This code can be used to quickly map the response ids of newly pulled data in the input file to 
the existing database. As a result it will create an enrichment folder in a given path with with 
an input file with Chattermill IDS. You only need to specify the location of the files and query parametrs.
Input file needs to have 'response_id' column 
"""


def two_dfs_simple(df_new, df_db, col_names, enrichment = True):
    
    df_final = pd.merge(df_new, df_db, on=[col_names[0]]) 
                  
    if not enrichment:
        df_final = df_new[~df_new[col_names[0]].isin(df_db[col_names[0]])]
 
    return df_final


def check_unique(df, col_name):
    if not (df[col_name].is_unique):
        msg = f'{col_name} field is unique.' 
    else: 
        msg = f'{col_name} field is not unique. Add another field.'    
    return msg




def discribe_result(df_final, df_new, df_db, col_names, enrichment):
    if enrichment:
        msg = f'{df_final.shape[0]} out of {df_new.shape[0]} rows were mapped. {df_new.shape[0]-df_final.shape[0]} were not.' 
    else:
        msg = f"""{df_final.shape[0]} out of {df_new.shape[0]} rows are unique. """
    return msg

    
def discribe_result2(df_final, df_new, df_db, col_names, enrichment):
    message1 = f'{col_names[0]} field is unique' if (df_db[col_names[0]].is_unique) else f'{col_names[0]} field is not unique. Add another field.'   
    message2 = f'{df_final.shape[0]} out of {df_new.shape[0]} rows were mapped' * enrichment or f'{df_final.shape[0]} out of {df_new.shape[0]} rows are unique'  
    return message1, message2

def two_dfs(df_new, df_db, col_names, enrichment = True):
    if enrichment:
        df = pd.merge(df_new, df_db, on=[col_names[0]])                   
    else:
        existing_ids = df_db[col_names[0]].tolist()
        df = df_new[~df[col_names[0]].isin(existing_ids)]  
    return df

def compare_ids(df_new, df_db, col_names,  enrichment = True):
        
    if len(col_names)==1: # if response ids are unique   
        df_db = pd.DataFrame(df_db, columns = ['id', col_names[0]])         
        df_new[col_names[0]] = df_new[col_names[0]].astype(str)
        
        if enrichment:
            df_final= pd.merge(df_new, df_db, on=[col_names[0]])                   
        else:
            existing_ids = df_db[col_names[0]].tolist()
            df_final = df_new[~df_new[col_names[0]].isin(existing_ids)]  
    else:
        df_new[col_names[0]] = df_new[col_names[0]].astype(str)
        df_new[col_names[1]] = df_new[col_names[1]].astype(str)
            
        if enrichment:
            df_final = pd.merge(df_new, df_db, on=[col_names[0], col_names[1]])      
            
        else:
            existing_ids = df_db[col_names[0]].tolist()
            existing_ids_2 = df_db[col_names[1]].tolist()     
            df_1 = df_new[~df_new[col_names[0]].isin(existing_ids)]   
            df_final = df_new[~df_new[col_names[1]].isin(existing_ids_2)]     

    print('Response ids are unique' * (df_db[col_names[0]].is_unique) or 'Response ids are not unique. Add another field.')   
    print(f'{df_final.shape[0]} out of {df_new.shape[0]} rows were mapped' * enrichment or f'{df_final.shape[0]} out of {df_new.shape[0]} rows are unique')    
    return df_final




