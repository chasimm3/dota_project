import requests

def make_api_request(self, url):
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an error for bad status code
        return response.json()
    except requests.RequestException as e:
        return {'error': str(e)}
    
    
# Function to get value based on column match
def get_value_by_column(df, column_name, match_value, return_column):
    # Filter the dataframe
    filtered_df = df.loc[df[column_name] == match_value]
    
    # Check if the filtered dataframe is not empty
    if not filtered_df.empty:
        # Return the value from the specified return column
        return filtered_df[return_column].values[0]
    else:
        return None
