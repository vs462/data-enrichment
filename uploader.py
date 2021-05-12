import pandas as pd
import streamlit as st
import enrichment_tool as enr
import json
import boto3
from io import StringIO    
    
def uploader(name):
    ''' retrieve data based on the selected option '''
    st.markdown(f'### {name} data', unsafe_allow_html=True)  
    
    if name =='New':
        options = (f'CSV/Excel Upload ({name})', 'JSON download', 'JSONL download', 'Fetch from S3')
    
    else:
        options = (f'CSV/Excel Upload ({name})', 'Pull from database')
        
    option = st.radio('', options)        
    
    if option == f'CSV/Excel Upload ({name})':
        df = from_csv(name)

    elif option == 'Pull from database':
        df = from_db()

    elif option == 'Fetch from S3':        
        df = from_s3()
    
    elif option == 'JSON download':        
        df = from_json()
    
    elif option == 'JSONL download':        
        df = from_jsonl()
        
    preview_df(df)
    
    return df

def preview_df(df):
    if df is not None:
        my_expander = st.beta_expander("Table preview", expanded=False)
        my_expander.dataframe(df)
    
    
def from_json():
    user_file = st.file_uploader("Upload JSON file", type="json")
    if user_file:
        responses = json.load(user_file) 
        df = convert_to_table(responses)
        return df
    return None
   
def from_jsonl():
    responses = []
    user_file = st.file_uploader("Upload JSONL file", type="jsonl") 
    if user_file:           
        for json_str in list(user_file) :                
            result = json.loads(json_str)
            responses.append(result)
        df = convert_to_table(responses)
        return df
    return None 
      
def convert_to_table(responses):    
    normalized_df = pd.json_normalize(responses)
    return normalized_df                        

def from_csv(name):
    ''' lets user upload csv and turns in into pandas df '''
    user_file = st.file_uploader(f"Upload a CSV/Excel file ({name})") 
    
    if user_file:
        user_filename = user_file.name.lower()
        
        if user_filename.split('.')[-1] == 'csv':                
            df = pd.read_csv(user_file)
            return df
        
        elif (user_filename.split('.')[-1] == 'xls') or (user_filename.split('.')[-1] == 'xlsx'):
            df = pd.read_excel(user_file)
            return df

        else:
            st.warning("you need to upload a csv or excel file.")
            return None
    return None
            
    
def from_db():
    q = '''e.g. select id, user_meta->'unique_id'->>'value' unique_id, user_meta->'question'->>'value' question
    from responses
    where dataset_id = 10052
    and created_at > '2021-05-10'
    limit 4;'''

    user_input = st.text_area("Write an SQL query", '', height = 200, help= q)   
    if user_input:
        df = enr.blazer_query(user_input)
        return df 


def from_s3():
    s3 = boto3.resource('s3') 
    bucket_name = list_buckets()
    object_key = st.text_input("Enter file name with prefix", '', help =' e.g. testing/df_sm_new.csv' )
    
    try:
        obj = s3.Object(bucket_name, object_key)
        body = obj.get()['Body'].read()
        df = pd.read_csv(StringIO(body.decode('utf-8')))
        return df
    except:
        if object_key:
            st.markdown(' ### File not found', unsafe_allow_html=True)

def list_buckets():
    s3 = boto3.client('s3')   
    response = s3.list_buckets()    
    all_buckets = [bucket["Name"] for bucket in response['Buckets']]
    bucket = st.selectbox('Choose bucket', all_buckets) 
    # all_in_bucket = [my_bucket_object for my_bucket_object in my_bucket.objects.all()]
    return bucket

    




    



