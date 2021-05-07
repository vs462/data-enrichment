import pandas as pd
import streamlit as st
#import enrichment_and_unique as enr

st.set_page_config(layout="wide")
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True) # make buttons display horizontally 
st.title('Map data / get unique ids')


       
def csv_upload(name):
    upload_new = st.file_uploader(f"Upload a CSV/Excel file ({name})")
    if upload_new:
        df_new = pd.read_csv(upload_new)
        return df_new    
    return None
      
def initialise():   
    
    st.markdown(' ## Choose tool', unsafe_allow_html=True)
    tool = st.radio("", ('Enrichment', 'Unique IDs'))
    
    enrichment = True if tool == 'Enrichment' else False  

    st.markdown(' ## Add data', unsafe_allow_html=True) 
    
    col1, col2 = st.beta_columns(2)
    with col1:
        df_new = get_data("New") 
    
    with col2:
        df_existing = get_data("Existing")  
    
    

def get_data(name):
    st.markdown(f'### {name} data', unsafe_allow_html=True)  
    second_opt = 'Fetch from API' if name =='New' else 'Pull from database'  
    option = st.radio('', (f'CSV Upload ({name})', second_opt))    
    if option == f'CSV Upload ({name})':
        df = csv_upload(name)
    
    elif option == 'Pull from database':
        st.markdown(' ### not available yet', unsafe_allow_html=True)
    
    elif option == 'Fetch from API':
        st.markdown(' ### not available yet :(', unsafe_allow_html=True)



initialise()





