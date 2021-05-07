import pandas as pd
import streamlit as st
import enrichment_and_unique as enr

st.set_page_config(layout="wide")
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True) # make buttons display horizontally 
st.title('Map data / get unique ids')


def initialise():     
    st.markdown(' ## Choose tool', unsafe_allow_html=True)
    tool = st.radio("", ('Enrichment', 'Unique IDs'))
    enrichment = True if tool == 'Enrichment' else False       
    st.markdown(' ## Add data', unsafe_allow_html=True)    
    df_new = load_data()


def csv_upload():
    upload_new = st.file_uploader("Upload a CSV/Excel file")
    if upload_new:
        df_new = pd.read_csv(upload_new)
        return df_new    
    return None
             
def load_data():
    col1, col2 = st.beta_columns(2)
    
    with col1:
        st.markdown(' ### New data', unsafe_allow_html=True)
        add_new = st.radio('', ('CSV new Upload', 'Fetch from an API'))  
        df_new = csv_upload() if (add_new == 'CSV new Upload') else None
        
        st.markdown(' ### Rename columns', unsafe_allow_html=True)

    with col2:
        st.markdown(' ### Existing data', unsafe_allow_html=True)
        add_existing = st.radio("", ('CSV Upload (existing data)', 'Pull from db'))
        df_db = csv_upload() if (add_existing == 'CSV Upload (existing data)') else None
        
        st.markdown(' ### Rename columns', unsafe_allow_html=True)
        

                
    st.markdown(' ### Run', unsafe_allow_html=True)

# df_merged = pd.merge(df_all, df_db, on = ['case_num'])

# existing_ids = df_db['case_num'].tolist()

# df_unique = df_all[~df_all['case_num'].isin(existing_ids)]



initialise()





