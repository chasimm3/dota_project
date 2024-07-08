import json
import pathlib
import pandas as pd
from pathlib import Path

# function to load Json data in a dataframe to a file 
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
        
# function to create a folder structure if it doesn't already exist
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
    


# function to load dataframe to parquet using gzip compression
def load_parquet(dataframe, file_name):
    try:
        dataframe.to_parquet(file_name, compression='gzip')
    except Exception as error:
        print(repr(error))


# function to load dataframe to csv using desired seperator
def load_csv(dataframe, file_name, seperator):
    try:
        dataframe.to_csv(file_name, index=False, sep=seperator)
    except Exception as error:
        print(repr(error))

# function to load data frame to excel file, can specify whether to append a new sheet to an existing file, or create a new document for the file
def load_excel(dataframe, file_name, sheet_name, single_file):
    try:
        # if not unified file, write to a new file with the specified file name
        if single_file == False:
            with pd.ExcelWriter(
                file_name + '.xlsx',
                mode='w',
                engine='openpyxl'
            ) as writer:
                dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        
        else:
        # if the file already exists, append a new sheet to the file (overwriting if the sheet already exists)
            if Path(file_name + '.xlsx').is_file():
                with pd.ExcelWriter(
                    file_name + '.xlsx',
                    mode='a',
                    engine='openpyxl',
                    if_sheet_exists='replace',
                ) as writer:
                    dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        # if the file doesn't already exist, create it and write the data to a named sheet
            else:
                with pd.ExcelWriter(
                    file_name + '.xlsx',
                    mode='w',
                    engine='openpyxl'
                ) as writer:
                    dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
    except Exception as error:
        print(repr(error))    