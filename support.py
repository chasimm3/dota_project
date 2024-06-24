import requests
import glob
import os
import pandas as pd
import json
import pathlib

def make_api_request(self, url):
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an error for bad status code
        return response.json()
    except requests.RequestException as e:
        return {'error': str(e)}
    
def load_json_to_file(json_data, file_path, mode=None):
    try:
        if mode != None:
            with open(file_path, mode) as file:
                json.dump(json_data, file, indent=4)
        else:
            with open(file_path, 'w') as file:
                json.dump(json_data, file, indent=4)
    except Exception as error:
        print('Error in file creation: ' + repr(error))
        
def create_folder(path):
    try:
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    except Exception as error:
        print(repr(error))
    
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


class DataManipulation():
    def __init__(self, data_folder, delta_folder):
        self.data_folder = data_folder
        self.delta_folder = delta_folder
    
    def get_last_run(self, file_prefix):
        
        self.file_prefix = file_prefix
        
        # search for files in the folder which contain the prefix
        search_pattern = os.path.join(self.data_folder, self.file_prefix + '*')
        list_of_files = glob.glob(search_pattern)
        
        # get latest file based on created date
        self.latest_file = max(list_of_files, key=os.path.getctime)
        
        # create a data frame of the latest file
        self.df_last = pd.read_csv(self.latest_file)
        
    def compare_data(self, file_prefix, df_old, df_new, date):
        
        self.file_prefix = file_prefix
        self.df_new = df_new
        self.date = date
        
        # initiate delta file name
        if date is not None:
            self.delta_file = self.delta_folder + self.file_prefix + '_deltas_' + self.date + '.csv'
        else:
            self.delta_file = self.delta_folder + self.file_prefix + '_deltas.csv'
        
        print(self.delta_file)
        
        # get different values between dataframes
        df_comp = pd.merge(df_old, self.df_new, how='outer', indicator='Change Type')
        df_comp = df_comp.loc[df_comp['Change Type'] != 'both']    
        
        # change the text of the indicator column "Change Type" to New or Deleted
        df_comp['Change Type'] = df_comp['Change Type'].str.replace('left_only', 'Deleted').str.replace('right_only', 'New')
        
        # output the delta to the delta folder
        df_comp.to_csv(self.delta_file, index=False, mode='w')    
        
