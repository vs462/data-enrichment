import pandas as pd
import streamlit as st
import enrichment_tool as enr

st.set_page_config(layout="wide")
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True) # make buttons display horizontally 
st.title('Map data / get unique ids')

            
def initialise():       
    st.markdown(' ## Choose tool', unsafe_allow_html=True)
    tool = st.radio("", ('Enrichment', 'Unique IDs'))
    enrichment = True if tool == 'Enrichment' else False  

    st.markdown(' ## Add data', unsafe_allow_html=True) 
    
    col1, col2 = st.beta_columns(2)
    
    analysed_col_name = 'analysed_col_name'
    
    with col1:
        df_new = upload_data("New")
    
    with col2:
        df_existing = get_data("Existing")        
        col_existing = select_columns(df_existing, "Existing")        
        warning = enr.check_unique(df_existing, col_existing)   
        warning
        df_existing[analysed_col_name] = df_existing[col_existing]
        
    col_names = ['analysed_col_name']

    if df_new is not None and df_existing is not None:
        st.markdown(' ## running', unsafe_allow_html=True) 
        df_final = enr.two_dfs_simple(df_new, df_existing, col_names, enrichment = enrichment)
        #st.write(df_final, unsafe_allow_html=True)   
        
        message = enr.discribe_result(df_final, df_new, df_existing, col_names, enrichment)
        st.markdown(f' ## {message}', unsafe_allow_html=True)
    
def select_columns(df, name):
    option = st.selectbox(f'Choose column to map from {name} data', (df.columns))
    return option


def upload_data(name):
    analysed_col_name = 'analysed_col_name'
    df = get_data(name)         
    col = select_columns(df, name)        
    warning = enr.check_unique(df, col) 
    warning
    df[analysed_col_name] = df[col]
    return df
    

def get_data(name):
    st.markdown(f'### {name} data', unsafe_allow_html=True)  
    second_opt = 'Fetch from API' if name =='New' else 'Pull from database'  
    option = st.radio('', (f'CSV Upload ({name})', second_opt))    
    
    if option == f'CSV Upload ({name})':
        df = csv_upload(name)
        
    
    elif option == 'Pull from database':
        st.markdown(' ### not available yet', unsafe_allow_html=True)
        df = None
    
    elif option == 'Fetch from API':
        st.markdown(' ### not available yet :(', unsafe_allow_html=True)
        df = None
        
    return df

def csv_upload(name):
    upload = st.file_uploader(f"Upload a CSV/Excel file ({name})")
    if upload:
        df = pd.read_csv(upload)
        return df 



initialise()





