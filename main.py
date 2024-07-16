import json
from datetime import datetime, timedelta
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

from modules.sources import wincher
from modules.utils.slack import api_fail_to_slack, auth_alert_to_slack, load_env_from_yaml, post_failed_insertion_to_slack
from modules.utils._credentials import *
import json
import os
from datetime import date, timedelta,time
from ssh.wincher_api_key import API_key
import time as t

def process_data(request):
    load_env_from_yaml('.env.yaml')
    website_id = '761559'
    credentials_instance = LocalCredentials()
    # run_locally = False

    #Checking if running locally or on cloud, setting credentials accordingly
    if run_locally:
        print('Running Locally...')
        access_token = f'{API_key}' 
    else:
        access_token = os.environ.get('WINCHER_API_TOKEN')
        print(f'Access token value: {access_token}')
        if access_token == None:
            auth_alert_to_slack(function_name='Wincher - Hoffman Keywords Data', table_name = 'hoffman_keywords_data', time=datetime.now()) 
            return
        
    #Defining the desired date range
    first_day_prev_month = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)
    last_day_prev_month = date.today().replace(day=1) - timedelta(days=1)
    
    #Creating Wincher Instance with the required params
    wincher_instance = wincher.Wincher(website_id, access_token, start_at= first_day_prev_month, end_at= last_day_prev_month)
    data_list= []   
    keywords_dataframe = []

    #Defining Big Query Setup
    dataset_id = 'wincher_data'
    keyword_table_id = 'hoffman_keywords_data'

    #Getting the data from the API
    try:
        print('Getting Keywords Data..')
        keywords_json = wincher_instance.get_tracked_keywords(ranking=True)

        for index,item in enumerate(keywords_json['data']):
            print(index)
            df_columns = {
            'cpc' : item['cpc'],
            'competition' : item['competition'],
            'volume' : item['volume'],
            'groups' : item['groups'],
            'difficulty' : item['difficulty'],
            'search_intents' : item['search_intents'],
            'ranking' : item['ranking'],
            'id' : item['id'],
            'keyword' : item['keyword'],
            'last_manual_refresh_at' : item['last_manual_refresh_at'],
            'ranking_updated_at' : item['ranking_updated_at'],
            'created_at' : item['created_at'],
            'preferred_url' : item['preferred_url'],
            }
            data_list.append(df_columns)
        print(keyword_table_id)
    except Exception as error:
        api_fail_to_slack(function_name='Wincher - Hoffman Keywords Data', table_name = 'hoffman_keywords_data', time=datetime.now()) 
        return


    #Creating a Dataframe from deserialized response
    print('Creating DataFrame...')
    keywords_dataframe = pd.DataFrame(data_list)

    #Upload DataFrame to BigQuery
    try:
        client = credentials_instance.bigquery_client()
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"  # Change to "WRITE_APPEND" if you want to append data
        )
        print('Uploading Keywords Data now...')
        job = client.load_table_from_dataframe(
            keywords_dataframe, f"{dataset_id}.{keyword_table_id}", job_config=job_config,
        )
        job.result()  # Wait for the job to complete
        print('Uploaded Keywords Data to BigQuery successfully done..')
    except Exception as error:  
        print(f'Following Error Occurred: {error}')
        post_failed_insertion_to_slack(function_name='Wincher - Hoffman Keywords Data', table_name = 'hoffman_keywords_data', time=datetime.now())