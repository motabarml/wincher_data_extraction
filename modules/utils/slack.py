import io
import json
import requests
import yaml
from typing import List, Dict
import matplotlib.pyplot as plt
import os

from modules.utils._credentials import LocalCredentials

#Import Credentials Object
def load_env_from_yaml(file_path):
    with open(file_path, 'r') as file:
        env_vars = yaml.safe_load(file)
        for key, value in env_vars.items():
            os.environ[key] = value
# Load the environment variables from .env.yaml cause os doesn't directly load from yaml

# Base function to send messages to Slack. It's just hitting the endpoint with the token and channel
def post_message_to_slack(text: str, blocks: List[Dict[str, str]] = None):
    load_env_from_yaml('.env.yaml')
    slack_app_token = os.getenv('SLACK_API_TOKEN')
    print(slack_app_token)
    slack_app_channel = os.environ.get('SLACK_APP_CHANNEL')
    print(slack_app_channel)

    
    return requests.post('https://slack.com/api/chat.postMessage', {
        'token': os.getenv("SLACK_APP_TOKEN"),
        'channel': os.getenv("SLACK_APP_CHANNEL"),
        'text': text,
        'blocks': json.dumps(blocks) if blocks else None
    }).json()	

# Function to send files through Slack
def post_file_to_slack(text: str, file_name: str, file_bytes: bytes, file_type: str = None, title: str = None):
    return requests.post(
        'https://slack.com/api/files.upload', 
        {
            'token': os.getenv("SLACK_APP_TOKEN"),
            'filename': file_name,
            'channels': os.getenv("SLACK_APP_CHANNEL"),
            'filetype': file_type,
            'initial_comment': text,
            'title': title
        },
        files={'file': file_bytes}
    ).json()



# Wrapper function to post Matplotlib plots to Slack
def post_matplotlib_to_slack():
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor="white")
    buf.seek(0)
    post_file_to_slack("", "", buf)



# Custom functions using Block Kit to structure the messages sent to Slack
# Process start
def post_start_process_to_slack(process_name: str, time):
    start_time = time
    start_block = [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "A new process has just started :rocket:",
        }
      },
      {
        "type": "section",
        "fields": [{
            "type": "mrkdwn",
            "text": f"Process _{process_name}_ started at {start_time} ⏳"
            }
        ]
        }
    ]

    post_message_to_slack("New process kicked off!", start_block)

# Process end
def post_end_process_to_slack(process_name: str, time):
    end_time = time
    end_block = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Process successful :large_green_circle:"
            }
        },
        {
            "type": "section",
            "fields": [{
                "type": "mrkdwn",
                "text": f"Process: _{process_name}_ finished successfully at {end_time} ✅"
            }]
        }
    ]
    post_message_to_slack("Process ended successfully", end_block)

# Process failed
def post_failed_process_to_slack(function_name,process_name: str, time):
    failed_time = time
    
    failed_block = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Process Failed! :large_red_circle:"
            }
        },
        {
            "type": "section",
            "fields": [{
                "type": "mrkdwn",
                "text": f"Process: _{process_name}_ failed successfully at {failed_time} ❌"
            }]
        }
    ]
    post_message_to_slack("Process failed!", failed_block)

#Post Failed Insertion
def auth_alert_to_slack(function_name,table_name: str, time):
    failed_block = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{function_name}: Something Went Wrong ⚠️"
            }
        },
        {
			"type": "divider"
		},
        {
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*Type:*\n Code: 400"
				},
				{
					"type": "mrkdwn",
					"text": f"*When:*\n {time}"
                    
				},

				{
					"type": "mrkdwn",
					"text": f"*Description:*\n Relevant table: *{table_name}*"
                    
				},

			]
		}
    ]
    post_message_to_slack("Code:400", failed_block)



#Post API Fail to slack
def api_fail_to_slack(function_name, table_name: str, time):
    failed_block = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{function_name}: Something Went Wrong ⚠️"
            }
        },
        {
			"type": "divider"
		},
        {
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*Type:*\nCode: 405"
				},
				{
					"type": "mrkdwn",
					"text": f"*When:*\n {time}"
                    
				},

				{
					"type": "mrkdwn",
					"text": f"*Description:*\nRelevant table: *{table_name}*"
                    
				},

			]
		}
    ]
    post_message_to_slack("Code: 405", failed_block)




#Post Failed Insertion
def post_failed_insertion_to_slack(function_name, table_name: str, time):
    failed_block = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{function_name}: Something Went Wrong ⚠️"
            }
        },
        {
			"type": "divider"
		},
        {
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*Type:*\nCode: 401"
				},
				{
					"type": "mrkdwn",
					"text": f"*When:*\n {time}"
                    
				},

				{
					"type": "mrkdwn",
					"text": f"*Description:*\nRelevant table: *{table_name}*"
                    
				},

			]
		}
    ]
    post_message_to_slack("Code: 401", failed_block)


        
#Post Failed Deletion
def post_failed_deletion_to_slack(function_name, table_name: str, time):
    failed_block = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{function_name}: Something Went Wrong ⚠️"
            }
        },
        {
			"type": "divider"
		},
        {
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*Type:*\nCode: 402"
				},
				{
					"type": "mrkdwn",
					"text": f"*When:*\n {time}"
                    
				},

				{
					"type": "mrkdwn",
					"text": f"*Description:*\nRelevant table: *{table_name}*"
                    
				},

			]
		}
    ]
    post_message_to_slack("Code: 402", failed_block)






