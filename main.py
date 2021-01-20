### Libraries 

# Dataframes
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import datetime

# SQL 
from sqlalchemy import create_engine
import psycopg2

#Google 
import gspread 
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import *

#Snowflake
from snowflake.sqlalchemy import ARRAY as Array,URL, TIMESTAMP_LTZ, TIMESTAMP_TZ, TIMESTAMP_NTZ


# Secret Manager

def access_secret_version(project_id, secret_id, version_id):
    from google.cloud import secretmanager
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    name = client.secret_version_path(project_id, secret_id, version_id)
    # Access the secret version.
    response = client.access_secret_version(name)
    # Print the secret payload.
    #
    # WARNING: Do not print the secret in a production environment - this
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode('UTF-8')
    return payload


from job_functions.gcp import access_secret_version
#secret manager
project_id = 'id'
user = 'user'
pwd = 'pass'
u = access_secret_version(project_id,user,2)
p = access_secret_version(project_id,pwd,2)
path = os.getcwd()





## Access Google Sheets function 

def extract_gs(key, sheet_name, action):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('json.pass', scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(key)
    sheet = sh.worksheet(sheet_name)
    if action == 'records':
        sheet_recs = sheet.get_all_records()
        df = pd.DataFrame(data=sheet_recs)
    else:
        sheet_values = sheet.get_all_values()
        df= pd.DataFrame(data=sheet_values)
    return df
    
    
##ETL Page one (blades)
# Retrieving spreadsheet and unpivot peices with dates to make flat table
 
def blade_jobs(df):
    blade_stuff =df
    blade_jobs = blade_stuff.loc[0:,2:]
    blade_jobs =blade_jobs.replace(r'^\s*$', np.nan, regex=True)
    blade_jobs=blade_jobs.dropna(axis=0, thresh =4)
    blade_jobs.columns=blade_jobs.iloc[1]
    blade_jobs=blade_jobs.drop([0,1] )
    blade_melt = pd.melt(blade_jobs,id_vars=['Customer & Location','Scope of Work', 'Notes'],var_name = 'dates', value_name = 'names')
    split = blade_melt.pipe(
        split_list_like,
        col="names",
        new_col_prefix="name",
        pat=",",)
    blade_name_melt=pd.melt(split,id_vars=['Customer & Location','Scope of Work', 'Notes' ,'dates', 'names'],var_name = 'name_numb', value_name = 'name_load')
    return blade_name_melt


#clean blade jobs. Clean out sub parts of the spreadsheet and keep only records of active blade techs.

def blade_name_clean(df, jobs):
    blade_stuff = df[[2,3]]
    
    #marks values
    emp_start =blade_stuff.index[blade_stuff[2] == 'List of Employees'].values.astype(int)[0]
    emp_end=blade_stuff.index[blade_stuff[2] == 'Past Employees'].values.astype(int)[0]
    #makes table
    name_lookup = blade_stuff.loc[emp_start:emp_end,2:3]
    name_lookup.rename(columns = {2:'name_load'})
    #merge on other table
    final_table= jobs.merge(name_lookup,how = 'left'
                            ,left_on = 'name_load', right_on =3) 
    return final_table

# splits names up
def split_list_like(df: pd.DataFrame, col: str, new_col_prefix: str, pat: str = None):
    df = df.copy()
    split_col = df[col].str.split(pat, expand=True)

    return df.assign(
        **{
            f"{new_col_prefix}_{x}": split_col.iloc[:, x]
            for x in range(split_col.shape[1])
        }
    )


## ETL of blade techs certifications 
# cleaned and transposed.  

def get_blade_certs(df):
    df = df.drop([0,1,3,42,43,44,45])
# removes other stuff
    df = df.drop([21,22], axis =1 )
    df_t = df
    df_t= df_t.transpose()

    df_t = df_t.drop([0,1,3])
    df_t= df_t.drop([17,21,27], axis =1)
    df_t.columns = df_t.iloc[0]
    df_t= df_t.dropna(axis =1, how='all' )
    df_t=df_t.rename(columns = {'':"Tech_Name"})
    melt_certs = pd.melt(df_t,id_vars=['Tech_Name'],var_name = 'cert_name', value_name = 'expiration_date')
    corrected_certs = melt_certs.replace(r'^\s*$', np.nan, regex=True)
    corrected_certs = corrected_certs.dropna()
    return corrected_certs




## ETL Attrributes for Certs
def cert_atts(df):
    df = df.drop([0,1,3,42,43,44,45])
    df = df.drop([21,22], axis =1)
    df = df[[1,2,3]]
    df =df.replace(r'^\s*$', np.nan, regex=True)
    df=df.dropna(axis =0, how ='any')
    df = df.rename(columns = {1:"Site", 2: "cert_name", 3:"Fequency"})
    return df





### ETL data from construction SHEET

# get users from construction sheet 

def user_cont_tracker(df):
    users= df[['Site','Senior', 'Employee/Tech' , 'Title']]
    return users.replace(r'^\s*$', np.nan, regex=True)

# get users from construction users    

def clean_contr_jobs(df):
    clean = df.drop(['Site','Senior','Validation' ,'Pay Type','Per Diem','Employee/Contract','Title'], axis =1)
    a = clean.columns.to_list()
    a2 = a[1:]
    melt= pd.melt(clean,id_vars='Employee/Tech', value_vars=a2)
    melt=melt.replace(r'^\s*$', np.nan, regex=True)
    return melt


#clean jobs

def clean_tech_jobs(df):
    cleaned = []
    jobs = df
    col_res=df.columns.to_list()
    for y in col_res[12:]:
        clean_cols = datetime.datetime.strptime('2020-'+y + '-1', "%Y-%W-%w")
        cleaned.append(clean_cols.strftime("%m/%d/%Y"))
    ind_cols = dict(zip(jobs.columns[12:], cleaned))
    df = df.rename(ind_cols, axis = 'columns')
    a = df.columns.to_list()
    a2 = a[12:]
    # melt table 
    melt = pd.melt(df,id_vars=['Technician'], value_vars=a2)
    corrected_jobs = melt.replace(r'^\s*$', np.nan, regex=True)

    
    return corrected_jobs


# get tech tracker users

def get_users(df):
    users =df[['Count','Manager','Field Supervisor','Division','Technician', 'GWO', 'Job specific experience', 'Turbine Platform Experience', 'Paycom Name']]
    return users.replace(r'^\s*$', np.nan, regex=True)



# get con users 

def user_cont_tracker(df):
    users= df[['Site','Senior', 'Employee/Tech' , 'Title']]
    return users.replace(r'^\s*$', np.nan, regex=True)
    

## create one large table to represent all workers from different areas

def union_jobs(df_blade_jobs,tech_jobs,cont_jobs):
    
    # techs
    tech1 = clean_tech_jobs(tech_jobs)
    tech1['type']= 'tech_tracker'
    tech1 = tech1.rename(columns = {'Technician': 'Tech','variable': 'dates'})
    tech1['dates'] =pd.to_datetime(tech1['dates'], format='%m%dd%Y', errors = 'ignore')
    
    # construction
    cont_jobs = clean_contr_jobs(cont_jobs)
    cont_jobs['type'] = 'construction'
    cont_jobs = cont_jobs.rename(columns ={"Employee/Tech": 'Tech','variable': 'dates'})
    cont_jobs['dates'] =pd.to_datetime(cont_jobs['dates'], format='%mm%dd%Y', errors = 'ignore')
    
    # blade techs
    df2=blade_name_clean(df_blade_jobs,blade_jobs(df_blade_jobs))
    df2['dates'] =pd.to_datetime(df2['dates'] + '/2020', format='%mm%dd%Y', errors = 'ignore')
    df_clean =df2.dropna()
    df_clean['type']= 'blade_tech'
    df_clean = df_clean.rename(columns ={2: 'Tech','Customer & Location':'value'})
    df_tech = df_clean[['Tech','dates','value','type']]
    
    #Union
    union = pd.concat([df_tech,cont_jobs,tech1 ], ignore_index=True)
    union['dates']= pd.to_datetime(union['dates'], errors ='ignore')
    union['dupe']=union.duplicated(subset = ['Tech', 'dates']) 
    ls = union[union['dupe'] == True]
    
    
    # Union Cleaning
    union = union.rename(columns ={'Tech': 'Tech','dates':'Dates', 'value':'Value', 'type':'Sheet', 'dupe':'Duplicated'})
    
    return union
    

#write to snowflake function

def to_snowflake(df,table,u,p,action):
    write_engine = create_engine(URL(
        user=u,
        password=p,
        account="account",
        database = 'database',
        role='user',
        schema='name',
        numpy=True
    ))
    connection = write_engine.connect()
    try:
        if action=='replace':
            connection.execute("DROP TABLE ANALYTICS.HARVEST_DB."+table)
        if len(df) > 15000:
            n_chunks = (len(df)//15000) + 1
            dfs = np.array_split(df,n_chunks)
            for df in dfs:
                df.to_sql(table, con=write_engine, index=False, if_exists='append')
        else:
            df.to_sql(table, con=write_engine, index=False, if_exists='append')
    finally:
        connection.close()
        write_engine.dispose()



# extract and aggregate  all jobs tables to snowflake 

df_tech_jobs = extract_gs('sheetid','Field Services Tracker', 'records')
df_cont_jobs = extract_gs('sheetid',  'Construction Tracker', 'records')
df_blade_jobs = extract_gs('sheetid',  'Blade Techs','else')
df_load_jobs = union_jobs(df_blade_jobs,df_tech_jobs,df_cont_jobs)

df_load_jobs



# extract and aggregate all users sub tables to snowflake 

df_tech_user = get_users(df_tech_jobs)
df_cont_user = user_cont_tracker(df_cont_jobs)
blade_certs = extract_gs('1MReJociV4BQWQ94MRvyhb_fo0De_FWVFC1S4KGgGDfU','Blade Certs', 'other')
bla =blade_name_clean(df_blade_jobs,blade_jobs(df_blade_jobs))
bla = bla.dropna()

bla = bla.rename(columns ={'Tech': 'Tech','dates':'Dates', 'value':'Value', 'type':'Sheet', 'dupe':'Duplicated'})
bla

# clean for tech user upload 

df_tech_user = df_tech_user.drop(['Count', 'GWO', 'Turbine Platform Experience', 'Paycom Name', 'Job specific experience'], axis = 'columns')
df_tech_user['sheet'] ='tech_tracker'
df_tech_user = df_tech_user.rename(columns ={'Technician': 'Tech', 'Field Supervisor': 'field_supervisor'})
df_tech_user

# clean for cont user

df_cont_user =df_cont_user.drop(['Site', 'Title'], axis = 'columns')
df_cont_user['Manager'] = np.NaN
df_cont_user['Division'] = 'Construction'
df_cont_user['sheet'] = 'construction_tracker'
df_cont_user = df_cont_user.rename(columns ={'Employee/Tech': 'Tech', 'Senior': 'field_supervisor'})
df_cont_user = df_cont_user[['Manager','field_supervisor', 'Division', 'Tech', 'sheet']]

# blade user clean up 

bla =bla.drop( ['Customer & Location','Scope of Work','Notes','Dates','names','name_numb','name_load',3 ] , axis = 1)
bla['Manager'] = np.NaN
bla['field_supervisor'] = np.NaN
bla['Division'] = 'Blades'
bla['sheet'] = 'blade_tracker'
bla = bla.rename(columns ={2: 'Tech'})
bla = bla[['Manager','field_supervisor', 'Division', 'Tech', 'sheet']]
bla= bla.drop_duplicates()

df_load_users = pd.concat([df_tech_user,df_cont_user,bla], ignore_index=True)
df_load_users

#table load
to_snowflake(df_load_jobs,'harvest_jobs',u,p,'replace')

# user load 
to_snowflake(df_load_users,'harvest_users',u,p,'replace')















