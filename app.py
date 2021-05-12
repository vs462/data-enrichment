import pandas as pd
import streamlit as st
import enrichment_tool as enr
import base64
import uploader


st.set_page_config(layout="wide")
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True) # make buttons display horizontally 
#st.markdown('<style>' + open('styles.css').read() + '</style>', unsafe_allow_html=True)
st.title('Map Data / Get Unique IDs tool')
            
def initialise():  
    st.markdown(' ## Choose tool', unsafe_allow_html=True)
    tool = st.radio("", ('Enrichment', 'Unique IDs'))
    st.markdown('<p class="sep-line"> </p>', unsafe_allow_html=True)
    enrichment = True if tool == 'Enrichment' else False  
    st.markdown(' ## Add data', unsafe_allow_html=True)     
    col_num = st.checkbox('Map on two columns')    
    col1, col2 = st.beta_columns(2) 
    col_names = ['analysed_col_name1', 'analysed_col_name2'] if col_num else ['analysed_col_name']
    
    with col1:
        df_new = uploader.uploader("New")
        analyse_df(df_new, "New", col_names)
        
    with col2:
        df_existing = uploader.uploader("Existing")
        analyse_df(df_existing, "Existing", col_names)
    
    st.markdown('<p class="sep-line"> </p>', unsafe_allow_html=True)
    if df_new is not None and df_existing is not None:  
        run_analysis(df_new, df_existing, col_names, enrichment)

def run_analysis(df_new, df_existing, col_names, enrichment):
    if st.checkbox('run'): #  st.button('run')
        st.markdown(' ## Results', unsafe_allow_html=True) 
        
        df_final = enr.compare_ids(df_new, df_existing, col_names, enrichment)         
        message = enr.discribe_result(df_final, df_new, df_existing, col_names, enrichment)
        
        st.markdown(f' ### {message}', unsafe_allow_html=True)    
        df_final.drop(columns=col_names, inplace = True)
        df_final.drop(columns=['_merge'], inplace = True)
        download_link(df_final, 'mapped_file.csv', 'Download file in csv')
        uploader.preview_df(df_final)



def analyse_df(df, name, col_names):
    ''' let user select what columns to map on and checks for duplicates '''
    analysed_col_name = col_names
    
    if df is not None:
        if len(col_names)== 1:
            st.markdown('### Choose column to map from data', unsafe_allow_html=True)
            cols = st.selectbox(f'Choose column to map from {name} data', (df.columns))        
            warning = enr.check_unique(df, [cols]) 
            st.markdown(warning, unsafe_allow_html=True)
            df[analysed_col_name[0]] = df[cols].astype(str)
        
        else:
            col1 = st.selectbox(f'Choose first column to map from {name} data', (df.columns))  
            col2 = st.selectbox(f'Choose second column to map from {name} data', (df.columns))
            warning = enr.check_unique(df, [col1, col2]) 
            
            st.markdown(warning, unsafe_allow_html=True)
            df[analysed_col_name[0]] = df[col1].astype(str)
            df[analysed_col_name[1]] = df[col2].astype(str)    
            cols = [col1, col2]
            
        if name == 'Existing':
            df.drop(columns=cols, inplace = True)
        return df
    

def download_link(object_to_download, download_filename, download_link_text):
    if object_to_download is not None:
        st.markdown(' ### Download file', unsafe_allow_html=True)
        if isinstance(object_to_download, pd.DataFrame):
            object_to_download = object_to_download.to_csv(index=False)
        b64 = base64.b64encode(object_to_download.encode()).decode() # some strings <-> bytes conversions necessary here    
        tmp_download_link = f'<a href="data:file/txt;base64,{b64}" download="{download_filename}">{download_link_text}</a>'
        st.markdown(tmp_download_link, unsafe_allow_html=True)

initialise()