import requests

class Wincher:
    
    def __init__(self, website_id, access_token, start_at, end_at) -> None:
        self.list_keywords_path = f"/websites/{website_id}/keywords"
        self.base_url = "https://api.wincher.com/v1"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "content-type": 'application/json'
        }
        self.start_at = start_at
        self.end_at = end_at
    
    def requests_request(self, url, method='GET', params=None, json=None):
        """generic request method"""
        response = requests.request(
            url=url,
            headers=self.headers,
            method=method,
            params=params,
            json=json,
            timeout= 10
        )
        return response
    
    def get_tracked_keywords(self, ranking:bool):
        url = f"{self.base_url}{self.list_keywords_path}"
        
        # Specify the date range in the API request
        params = {
            'start_at': self.start_at,
            'end_at': self.end_at,
            'include_ranking': ranking,
        }
        
        response = self.requests_request(url, params=params)
        
        if response.status_code == 200:
            keywords_data = response.json()
            return keywords_data
        else:
            print(f"Error: Unable to fetch keywords data. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return None
        
    def get_keywords_data(self,website_id,keyword_id):
        url = f"{self.base_url}/websites/{website_id}/keywords/{keyword_id}"
        # Specify the date range in the API request
        params = {
            'start_at': self.start_at,
            'end_at': self.end_at
        }
        
        response = self.requests_request(url, params=params)
        
        if response.status_code == 200:
            keywords_data = response.json()
            return keywords_data
        else:
            print(f"Error: Couldn't fetch keywords data. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return None

    def list_keyword_groups(self,website_id):
        url = f'{self.base_url}/websites/{website_id}/groups'
        # Specify the date range in the API request
        params = {
            'start_at': self.start_at,
            'end_at': self.end_at,
            'include_ranking': True
            
        }
        response = self.requests_request(url, params=params)
        
        if response.status_code == 200:
            keywords_data = response.json()
            return keywords_data
        else:
            print(f"Error: Couldn't fetch keywords data. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return None

    
    def get_keyword_group(self,website_id,group_id):
        url = f"{self.base_url}/websites/{website_id}/groups/{group_id}?include_ranking=true"
        # Specify the date range in the API request
        params = {
            'start_at': self.start_at,
            'end_at': self.end_at,
        }
        
        response = self.requests_request(url, params=params)
        
        if response.status_code == 200:
            keywords_data = response.json()
            return keywords_data
        else:
            print(f"Error: Couldn't fetch keywords data. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return None
        
    def get_projects(self):
        url = 'https://api.wincher.com/v1/projects'
        params = {
            'start_at': self.start_at,
            'end_at': self.end_at,
        }
        response = self.requests_request(url, params=params)
        
        if response.status_code == 200:
            keywords_data = response.json()
            return keywords_data
        else:
            print(f"Error: Couldn't fetch keywords data. Status code: {response.status_code}")
            print(f"Response content: {response.text}")
            return None

   