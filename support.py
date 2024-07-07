import json
import pathlib
import pandas as pd


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


def load_parquet(dataframe, file_name):
    dataframe.to_parquet(file_name, compression='gzip')
    
def load_csv(dataframe, file_name, seperator):
    dataframe.to_csv(file_name, index=False, sep=seperator)