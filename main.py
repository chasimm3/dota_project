import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
import json
import glob
import os
import time
import numpy as np
from support import get_value_by_column, DataManipulation, load_json_to_file, create_folder


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

        with open(items_file_path, "r") as json_file:
            items_data = json.load(json_file)
            
        df_items = pd.DataFrame(items_data)
        
        print(df_items)
        
    
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
create_folder(delta_folder)
create_folder(data_folder)
create_folder(tables_folder)

open_dota = OpenDota(data_folder, delta_folder, staging_folder, tables_folder, date1)

# open_dota.load_parent('heroes', 'heroes/heroes_')
# open_dota.load_parent('proPlayers', 'pro_players/pro_players_')
# open_dota.load_parent('constants/items', 'constants/items/items_')
# open_dota.load_parent('constants/item_ids', 'constants/item_ids/item_ids_')

# open_dota.load_matches() # takes fucking ages

# # build dim_player
# open_dota.transform_dimension(staging_folder + 'pro_players/', tables_folder + 'dim_players.csv', 'dim_player_id')
# # build dim_hero
# open_dota.transform_dimension(staging_folder + 'heroes/', tables_folder + 'dim_heroes.csv', 'dim_hero_id')
# open_dota.transform_matches()

open_dota.transform_items()

