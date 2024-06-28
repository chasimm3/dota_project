import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
import json
import glob
import os
import time
import numpy as np
from support import get_value_by_column, DataManipulation, load_json_to_file, create_folder, flatten_json


class OpenDota():
    def __init__(self, data_folder, delta_folder, staging_folder, tables_folder, date1):
        
        # initialise variables
        self.base_url = 'https://api.opendota.com/api/'
        self.data_folder = data_folder
        self.delta_folder = delta_folder
        self.staging_folder = staging_folder
        self.tables_folder = tables_folder
    
    def load_parent(self, url_x, folder):
        # initialize variables
        url = self.base_url + url_x
        file_path = self.staging_folder + folder + date1 + '.json'
        
        # send api request to url
        response = requests.get(url)
    
        # convert response to json
        new_data = response.json()

        # load json to file path
        load_json_to_file(new_data, file_path, 'w')
    
        print("New file created: " + str(file_path))
       
            
    def load_matches(self):
        output_file = 'recent_matches_'
        output_folder = self.staging_folder + 'recent_matches/' + output_file
        players_file_path = self.staging_folder + 'pro_players/pro_players_' + date1 +'.json'
        
        # open the players file
        with open(players_file_path, "r") as json_file:
            data = json.load(json_file)
        
        i = 0
        
        # initialise dataframe of accountids and last match time from json data in staging file
        # df_columns = ('account_id', 'last_match_time')
        df = pd.json_normalize(data)
                
        # add column to dataframe containing url to players recent matches
        df['recent_matches_url'] = df.apply(lambda row: self.base_url + 'players/' + str(row.account_id) + '/recentMatches/', axis=1)
        
        for index, row in df.iterrows():
            last_match_time = row['last_match_time']
            last_match_time_str = str(row['last_match_time'])
            account_id = row['account_id']
            
            if not (last_match_time is None and type(last_match_time_str != None)):
                last_match_time = parser.parse(last_match_time_str)
                current_date = datetime.now(last_match_time.tzinfo)                        
                
                # calculate date difference between last match and now, if less than 5 hours ago, build the api url
                date_difference = current_date - last_match_time
                if (date_difference < timedelta(hours=5)):# and player_name == 'SaberLight':
                    
                    print(row['recent_matches_url'] + ' ' + str(account_id))
                    self.matches = requests.get(row['recent_matches_url']).json()

                    with open(output_folder + str(account_id) + '.json', "w") as f:
                        json.dump(self.matches, f, ensure_ascii=False, indent=4)            
                    time.sleep(1)         
        
    def transform_dimension(self, file_path, output_file_path, pk_name):
        
        list_of_files = glob.glob(file_path + '*') # * means all, if need specific format then *.csv
        latest_file = max(list_of_files, key=os.path.getctime)
        print(latest_file)
        
        with open(latest_file, "r") as json_file:
            data = json.load(json_file)
            
        # initialise dataframe of player data in the staging file
        df = pd.json_normalize(data)
        
        
        # # add effective to date column on the end
        current_date = datetime.now()
        df['load_date'] = pd.Timestamp(current_date)
        
        # insert incremental integer in column 1 
        df.insert(0, pk_name, range(1, 1 + len(df)))
        
        # load to csv, not including the index (I know I just reset it, it was for testing purposes)
        df.to_csv(output_file_path, index=False)
        
        print('Player Transformation Complete: ' + output_file_path + ' created.')
        
        
    def transform_matches(self):
        
        matches_file_path = self.staging_folder + 'recent_matches/'
        output_file = self.tables_folder + 'fact_matches.csv'
        
        list_of_files = glob.glob(matches_file_path + '*')      
        
        df = pd.DataFrame()
        
        # load players csv file to dataframe            
        players_file = self.tables_folder + 'dim_players.csv'
        df_players = pd.DataFrame()
        df_players = pd.read_csv(players_file, header='infer')
        
        # loop through json files in staging folder
        for i in list_of_files:
            with open(i, "r") as json_file:
                match_data = json.load(json_file)
                
                # append json array items to dataframe
                df2 = pd.json_normalize(match_data)
                
                # get account_id from the file name and add it as an additional columns

                # -> reverse the path 
                # -> within the reverse of the path find the location of the first _ 
                # -> take everything to the left of the first underscore 
                # -> reverse it back
                # -> replace .json with nothing
                
                u = (((i[::-1])[:((i[::-1]).find('_'))])[::-1]).replace('.json', '')
                
                df2['account_id'] = u
                
                # get the dim_player_id for the account id
                df2['fk_dim_player_id'] = get_value_by_column(df_players, 'account_id', int(u), 'dim_player_id')
                
                df = pd.concat([df2, df])

        current_date = datetime.now()
        df['effective_from_date'] = pd.Timestamp(current_date)
        
        # save dataframe to csv file
        df.to_csv(output_file, index=False)
        
        print('Match Transformation Complete: ' + output_file + ' created.')
        
    def transform_items(self):
    
        items_file_path = self.staging_folder + 'constants/items/'
        item_ids_file_path = self.staging_folder + 'constants/item_ids/'
        output_file = self.tables_folder + 'dim_items.csv'    
        
        # open latest items file and write to dataframe
        list_of_files = glob.glob(items_file_path + '*') # * means all, if need specific format then *.csv
        items_latest_file = max(list_of_files, key=os.path.getctime)
        print(items_latest_file)
        with open(items_latest_file, "r") as json_file:
            items_data = json.load(json_file)
        
        df_items = pd.DataFrame(items_data).transpose().reset_index().rename(columns={"index":"item_name"})
        # df_items = df_items.transpose()
        # df_items = df_items.reset_index()
        # df_items = df_items.rename(columns={'index':'item_name'})
        
        df_abilities = []
        df_abilities = (pd.concat({i: pd.DataFrame(x) for i, x in df_items.pop('abilities').items()})
         .reset_index(level=1, drop=True)
         .join(df_abilities)
         .reset_index(drop=True).rename(columns={'title':'item_name'}).reindex())

        df_attrib = []
        # df_attrib = (pd.concat({i: pd.DataFrame(x) for i, x in df_items.pop('attrib').items()})
        #  .reset_index(level=1, drop=True)
        #  .join(df_attrib)
        #  .reset_index(drop=True).reindex())
        # print(df_attrib)
        
        

        # Extract 'attrib' and 'item_name' columns from df_items
        attrib_series = df_items['attrib']
        item_name_series = df_items['item_name']
        
        print(attrib_series)
        df_attrib_list = pd.concat({i: pd.DataFrame(x) for i, x in attrib_series.items()})

        # # Create a DataFrame from the 'attrib' column dictionaries
        # df_attrib_list = [pd.DataFrame(x, index=[i]) for i, x in attrib_series.items()]

        
        print(df_attrib_list)
        
            
            
        # # Concatenate the DataFrames in the list
        df_attrib = pd.concat(df_attrib_list)

        # # Reset the index to make it contiguous
        # df_attrib = df_attrib.reset_index(drop=True)

        # # Map the original index to the item names
        # df_attrib['item_name'] = [item_name_series[i] for i in df_attrib.index // len(df_attrib_list[0])]

        # # Reindex to ensure a contiguous index
        # df_attrib = df_attrib.reindex()
                
        # print(df_attrib)
        
        # df_attrib = df_attrib.transpose()

        # df_attrib.columns = df_attrib.iloc[0]
        # df_attrib = df_attrib.iloc[1:].rename_axis(None, axis=1).reset_index()
        # df_attrib = df_attrib[:-1].drop("index", axis=1)
        
        # print(df_attrib)        
        
        # # df_items['attrib'] = df_items['attrib'].apply(add_apostrophes)
        # df_items['attrib'] = df_items['attrib'].str.replace("'", '"').to_csv(self.staging_folder + 'output.csv', sep='|', index=False)
        # df_items['attrib'].to_csv(self.staging_folder + 'output.csv', sep='|', index=False)
        # print(df_items['attrib'].iloc[0])
        
        # df_items.to_csv(self.staging_folder + 'output.csv', sep='|', index=False)
        
        # # Convert JSON strings to lists of dictionaries

        # # Step 1: Convert JSON strings to lists of dictionaries
        # df_items['attrib'] = df_items['attrib'].apply(json.loads)

        # # Step 2: Flatten the JSON arrays
        # df_exploded = df_items.explode('attrib')

        # # Step 3: Normalize the JSON objects into separate columns
        # df_flattened = pd.concat([df_exploded.drop(columns=['attrib']), df_exploded['attrib'].apply(pd.Series)], axis=1)
        # '[{'key': 'blink_range', 'value': '1200'}, {'key': 'blink_damage_cooldown', 'value': '3.0'}, {'key': 'blink_range_clamp', 'value': '960'}]'
        
        # print(df_flattened)
        
        
        # for i, row in df_items.iterrows():
        #     print(row.attrib)
        
        #     # Step 1: Convert JSON strings to lists of dictionaries
        #     df_attrib = pd.json_normalize(row.attrib)
        #     print(df_attrib)


        
        
        
        
        
        
        
        
        
        
        # Replace underscores with spaces in the 'item_name' column
        df_items['item_name'] = df_items['item_name'].str.replace('_', ' ').str.lower()

        # Convert both columns to lower case
        # df_items['item_name'] = df_items['item_name'].str.lower()
        df_abilities['item_name'] = df_abilities['item_name'].str.lower()

        # Perform the merge on the lower case columns
        df_merged = pd.merge(df_items, df_abilities, left_on='item_name', right_on='item_name')

        # print(df_merged)
        
        
        
        #  open latest item_ids file and write to dataframe
        list_of_files = glob.glob(item_ids_file_path + '*') # * means all, if need specific format then *.csv
        item_ids_latest_file = max(list_of_files, key=os.path.getctime)
        
        with open(item_ids_latest_file, "r") as json_file:
            item_ids_data = json.load(json_file)
            
        
        # pivot the item_ids dataframe into 2 columns, item_id and item_name
        df_item_ids = pd.DataFrame(item_ids_data, index=[0])
        
        df_item_ids = df_item_ids.melt(var_name='item_id', value_name='item_name')
        
        
        
        
        # df_items.to_csv(self.staging_folder + 'output.csv', sep='|', index=False)
            
        # # List to store all items
        # all_items = []

        # # Iterate through each top-level key (item name)
        # for item_name, item_data in items_data.items():
        #     flattened_data = flatten_json(item_data, item_name)
        #     all_items.append(flattened_data)

        # # Convert the list of dictionaries to a DataFrame
        # df = pd.DataFrame(all_items)

        # # Ensure 'item_name' is the first column
        # columns = ['item_name'] + [col for col in df.columns if col != 'item_name']
        # df = df[columns]
        
        # print(df)

        
# Function to add apostrophes
def add_apostrophes(s):
    return f"'{s}'"               
            

    
base_file_path = 'D:/General Storage/Python/Liquipedia Data Grab/dota_project/'         
# base_file_path = 'C:/Work/Python/Dota/dota_project/'
data_folder = base_file_path + 'Data/'
delta_folder = base_file_path + 'Delta/'
staging_folder = base_file_path + 'Staging/'
tables_folder = base_file_path + 'Tables/'
base_url = 'https://api.opendota.com/api/'
player_file_prefix = 'players_sorted_by_country'

# get today's date
date1 = datetime.today().strftime('%Y%m%d')  # use ('%Y%m%d') when live so that it only loads once per day

# create folders if they don't exist2
create_folder(staging_folder + 'pro_players/')
create_folder(staging_folder + 'recent_matches/')
create_folder(staging_folder + 'heroes/')
create_folder(staging_folder + 'constants/items/')
create_folder(staging_folder + 'constants/item_ids/')
create_folder(staging_folder + 'constants/patchnotes/')
create_folder(delta_folder)
create_folder(data_folder)
create_folder(tables_folder)

open_dota = OpenDota(data_folder, delta_folder, staging_folder, tables_folder, date1)

# open_dota.load_parent('heroes', 'heroes/heroes_')
# open_dota.load_parent('proPlayers', 'pro_players/pro_players_')
# open_dota.load_parent('constants/items', 'constants/items/items_')
# open_dota.load_parent('constants/item_ids', 'constants/item_ids/item_ids_')
# open_dota.load_parent('constants/patchnotes', 'constants/patchnotes/patchnotes_')
# open_dota.load_matches() # takes fucking ages

# # build dim_player
# open_dota.transform_dimension(staging_folder + 'pro_players/', tables_folder + 'dim_players.csv', 'dim_player_id')
# # build dim_hero
# open_dota.transform_dimension(staging_folder + 'heroes/', tables_folder + 'dim_heroes.csv', 'dim_hero_id')
# open_dota.transform_matches()

open_dota.transform_items()

