import pandas as pd
<<<<<<< HEAD
import psycopg2
import streamlit as st
from secrete import dbname, user, password, port
import psycopg2

""" 
This code can be used to quickly map the response ids of newly pulled data in the input file to 
the existing database.
"""

def blazer_query(q):
    conn_string = f"host=127.0.0.1 dbname={dbname} user={user} password={password} port={port}" 
    conn = psycopg2.connect(conn_string) 
    cur = conn.cursor() 
    try:
        cur.execute(q) 
        conn.commit() 
        data = cur.fetchall() 
        colnames = [desc[0] for desc in cur.description]
    except Exception as e: 
        st.markdown(f'Invalid query. Error: {e}', unsafe_allow_html=True)
        return None
    cur.close()
    conn.close() 
    df = pd.DataFrame(data, columns = colnames)
    return df


def check_unique(df, col_name):
    ''' check if values in a column are unique and count duplicates '''
    dupl = df.pivot_table(index=col_name, aggfunc='size')

def check_unique(df, col_name):
    ''' check if values in a column are unique and count duplicates '''
    dupl = df.pivot_table(index=col_name, aggfunc='size')
    dup_num = (dupl > 1).sum()  
    dupl.to_csv('test.csv')

    if dup_num == 0:
        msg = f'{col_name} field is unique.' 
    else: 
        msg = f'{col_name} field is not unique ({dup_num} duplicates found). Add another field.'    
    return msg


def compare_ids(df_new, df_db, col_names, enrichment = True):
    ''' enrich dfs based on one column '''
    if len(col_names) == 1:
        df_merged = pd.merge(df_new, df_db, on=col_names[0], how='outer', indicator=True)        
    else:
        df_merged = pd.merge(df_new, df_db, on=col_names, how='outer', indicator=True)
        
    if enrichment:
        df_final = df_merged[df_merged['_merge'] =='both']
    else:
        df_final = df_merged[df_merged['_merge'] =='left_only']

    return df_final   

def discribe_result(df_final, df_new, df_db, col_names, enrichment):
    ''' returns how many rows were mapped and count duplicates in the resultant file '''
    dupl = df_final.pivot_table(index=col_names, aggfunc='size')
    dup_num = (dupl > 1).sum()
    if enrichment:        
        missing_rows = df_new.shape[0]-df_final.shape[0]
        
        msg = f"""{df_final.shape[0]} out of {df_new.shape[0]} rows were mapped; of those {dup_num} are duplicates. """
        
        if missing_rows > 0:
            msg += f'Missing {missing_rows} rows"'
        
    else:
        msg = f"""{df_final.shape[0]} out of {df_new.shape[0]} rows are not in our database; of those {dup_num} are duplicates."""
    return msg
    
    if len(col_names) == 1:
        df_merged = pd.merge(df_new, df_db, on=col_names[0], how='outer', indicator=True)        
    else:
        df_merged = pd.merge(df_new, df_db, on=col_names, how='outer', indicator=True)
        
    if enrichment:
        df_final = df_merged[df_merged['_merge'] =='both']
    else:
        df_final = df_merged[df_merged['_merge'] =='left_only']

    return df_final   

def discribe_result(df_final, df_new, df_db, col_names, enrichment):
    ''' returns how many rows were mapped and count duplicates in the resultant file '''
    dupl = df_final.pivot_table(index=col_names, aggfunc='size')
    dup_num = (dupl > 1).sum()
    if enrichment:
        msg = f'{df_final.shape[0]} out of {df_new.shape[0]} rows were mapped; of those {dup_num} are duplicates. Missing {df_new.shape[0]-df_final.shape[0]}' 
    else:
        msg = f'{df_final.shape[0]} out of {df_new.shape[0]} rows are not in our database; of those {dup_num} are duplicates.'
    return msg
