import pandas as pd
import streamlit as st
import enrichment_tool as enr
import base64

st.set_page_config(layout="wide")
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True) # make buttons display horizontally 
st.title('Map data / get unique ids')

            
def initialise():    

    st.markdown(' ## Choose tool', unsafe_allow_html=True)
    tool = st.radio("", ('Enrichment', 'Unique IDs'))
    enrichment = True if tool == 'Enrichment' else False  
    st.markdown(' ## Add data', unsafe_allow_html=True) 
    
    col_num = st.checkbox('Map on two columns')    
    
    col1, col2 = st.beta_columns(2) 
    col_names = ['analysed_col_name1', 'analysed_col_name2'] if col_num else ['analysed_col_name']
    
    with col1:
        df_new = upload_data("New", col_names)
    
    with col2:
        df_existing = upload_data("Existing", col_names)

    if df_new is not None and df_existing is not None:    
        if st.checkbox('run'):
            df_final = enr.compare_ids(df_new, df_existing, col_names, enrichment = enrichment)        
            message = enr.discribe_result(df_final, df_new, df_existing, col_names, enrichment)
            st.markdown(f' ## {message}', unsafe_allow_html=True)    
            df_final.drop(columns=col_names, inplace = True)
            download_link(df_final, 'mapped_file.csv', 'Download file in csv')
        


def upload_data(name, col_names):
    ''' initialises data uploader function, let user select what columns to map on and checks for duplicates '''
    analysed_col_name = 'analysed_col_name'
    analysed_col_name = col_names
    df = uploader(name) 
    
    if df is not None:
        if len(col_names)== 1:
            cols = st.selectbox(f'Choose column to map from {name} data', (df.columns))        
            warning = enr.check_unique(df, [cols]) 
            warning
            df[analysed_col_name] = df[cols].astype(str)
        
        else:
            col1 = st.selectbox(f'Choose first column to map from {name} data', (df.columns))  
            col2 = st.selectbox(f'Choose second column to map from {name} data', (df.columns))
            warning = enr.check_unique(df, [col1, col2]) 
            warning

            df[analysed_col_name[0]] = df[col1].astype(str)
            df[analysed_col_name[1]] = df[col2].astype(str)
            
            cols = [col1, col2]
            
        if name == 'Existing':
            df.drop(columns=cols, inplace = True)
        return df
      

def uploader(name):
    ''' retrieve data based on the selected option '''
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
    ''' lets user upload csv and turns in into pandas df '''
    upload = st.file_uploader(f"Upload a CSV/Excel file ({name})")
    if upload:
        df = pd.read_csv(upload)
        return df 

def download_link(object_to_download, download_filename, download_link_text):
    if object_to_download is not None:
        st.markdown(' ## Download file', unsafe_allow_html=True)
        if isinstance(object_to_download, pd.DataFrame):
            object_to_download = object_to_download.to_csv(index=False)
        b64 = base64.b64encode(object_to_download.encode()).decode() # some strings <-> bytes conversions necessary here    
        tmp_download_link = f'<a href="data:file/txt;base64,{b64}" download="{download_filename}">{download_link_text}</a>'
        st.markdown(tmp_download_link, unsafe_allow_html=True)

def test(df_new, df_existing, col_names, enrichment = True):
    df_new.index = df_new[col_names[0]]
    df_existing.index = df_existing[col_names[0]]  
    joined_df = df_new.join(df_existing, how='left')
    joined_df
    
initialise()



    



