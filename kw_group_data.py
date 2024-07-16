import json
from datetime import datetime, timedelta
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

from modules.sources import wincher
from modules.utils.slack import api_fail_to_slack, auth_alert_to_slack, load_env_from_yaml, post_failed_insertion_to_slack
from modules.utils._credentials import *
import os
from datetime import date, timedelta
from ssh.wincher_api_key import API_key
import yaml

def process_data():
    website_id = '761559'
    credentials_instance = LocalCredentials()

    # Checking if running locally or on cloud, setting credentials accordingly
    if run_locally:
        print('Running Locally...')
        access_token = f'{API_key}'
    else:
        access_token = os.getenv('WINCHER_API_TOKEN')
        print(access_token)
        if access_token is None:
            auth_alert_to_slack(
                function_name='Wincher - Hoffman Keywords Group Data',
                table_name='hoffman_keyword_group_data',
                time=datetime.now()
            )
            return

    # Defining the desired date range
    first_day_prev_month = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1)
    last_day_prev_month = date.today().replace(day=1) - timedelta(days=1)

    # Creating Wincher Instance with the required params
    wincher_instance = wincher.Wincher(website_id, access_token, start_at=first_day_prev_month, end_at=last_day_prev_month)
    group_data_list = []

    # Defining BigQuery Setup
    dataset_id = 'wincher_data'
    keyword_group_table_id = 'hoffman_keyword_group_data'

    # Getting the data from the API
    try:
        print('Getting Keywords Group Data...')
        keywords_group_json = wincher_instance.list_keyword_groups(website_id=website_id)
        
        for index, item in enumerate(keywords_group_json['data']):
            print(index)
            df_columns = {
                'id': item['id'],
                'name': item['name'],
                'created_at': item['created_at'],
                'updated_at': item['updated_at'],
                'keyword_ids': item['keyword_ids'],
                'ranking': item['ranking'],
                'avg_keyword_difficulty': item['avg_keyword_difficulty'],
                'search_intent_counts': item['search_intent_counts'],
            }
            group_data_list.append(df_columns)
    except Exception as error:
        api_fail_to_slack(
            function_name='Wincher - Hoffman Keywords Group Data',
            table_name='hoffman_keyword_group_data',
            time=datetime.now()
        )
        return
    
    # Creating a DataFrame from deserialized response
    print('Creating DataFrame...')
    keywords_group_dataframe = pd.DataFrame(group_data_list)

    # Upload DataFrame to BigQuery
    try:
        client = credentials_instance.bigquery_client()
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"  # Change to "WRITE_APPEND" if you want to append data
        )
        print('Uploading Keywords Group Data now...')
        job = client.load_table_from_dataframe(
            keywords_group_dataframe, f"{dataset_id}.{keyword_group_table_id}", job_config=job_config,
        )
        job.result()  # Wait for the job to complete
        print('Uploaded Keywords Group Data to BigQuery successfully.')
    except Exception as error:
        print(f'Following Error Occurred: {error}')
        post_failed_insertion_to_slack(
            function_name='Wincher - Hoffman Keywords Group Data',
            table_name='hoffman_keyword_group_data',
            time=datetime.now()
        )
