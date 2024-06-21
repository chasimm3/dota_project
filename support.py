import requests

def make_api_request(self, url):
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an error for bad status code
        return response.json()
    except requests.RequestException as e:
        return {'error': str(e)}