import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser
import json
import glob
import os
import time
import pathlib
from support import make_api_request, get_value_by_column, DataManipulation
from collections import defaultdict


class OpenDota():
    def __init__(self, data_folder, delta_folder, staging_folder, tables_folder, date1):
        
        # initialise variables
        self.base_url = 'https://api.opendota.com/api/'
        self.data_folder = data_folder
        self.delta_folder = delta_folder
        self.staging_folder = staging_folder
        self.tables_folder = tables_folder
        
        
    def load_players(self):
        
        self.pro_player_url = 'proPlayers/'
        self.players_file_path = self.staging_folder + 'pro_players/pro_players_' + date1 +'.json'
        
        if not os.path.exists(self.players_file_path):
                
            # api request to get pro players from Opendota
            self.response = requests.get(self.base_url + self.pro_player_url)        
            
            # check if response is valid
            # if self.response.status_code == 200:
            new_data = self.response.json()
     
            # overwrite the existing data with the new data
            with open(self.players_file_path, "w") as file:
                json.dump(new_data, file, indent=4)
                
            print("New File Created: " + str(self.players_file_path))
            
            
    def load_matches(self):
        output_file = 'recent_matches_'
        output_folder = self.staging_folder + 'recent_matches/' + output_file
        
        # open the players file
        with open(self.players_file_path, "r") as json_file:
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
        
    def transform_players(self):
        
        staging_file_path = self.staging_folder + 'pro_players/'
        output_file = self.tables_folder + 'dim_players.csv'
        
        list_of_files = glob.glob(staging_file_path + '*') # * means all, if need specific format then *.csv
        latest_file = max(list_of_files, key=os.path.getctime)
        print(latest_file)
           
        
        # open existing dimension and store in dataframea
        df_old = pd.read_csv(output_file, dtype=str, keep_default_na=False)
         
        # df_old = pd.read_csv(output_file)   
        
        # open the source file
        with open(latest_file, "r") as json_file:
            player_data = json.load(json_file)

        # initialise dataframe of player data in the staging file
        df = pd.json_normalize(player_data)
        
        
        # add effective to date column on the end
        current_date = datetime.now()
        df['effective_from_date'] = pd.Timestamp(current_date)
        df['effective_to_date'] = pd.Timestamp('3000-12-31')
        
        
        df_old = df_old[df_old.columns.difference(['effective_from_date', 'effective_to_date', 'dim_player_id'], sort=False)]
        
        # store new records in a dataframe
        df_new = pd.DataFrame().reindex_like(df_old)
        # df_new = df[df.columns.difference(['effective_from_date', 'effective_to_date'], sort=False)]
        
        df_new = pd.concat([df_new, df])
        
        
        df_old = df_old.convert_dtypes()
        
        print(df_new['steamid'])
        print(type(df_new['steamid']))
        print(df_old['steamid'])
        print(type(df_old['steamid']))
        
        #DataManipulation(self.tables_folder + 'dim_players.csv', self.tables_folder + '/delta/').compare_data('dim_players', df_old, df_new, str(current_date))
        
        # insert incremental integer in column 1 
        df.insert(0, 'dim_player_id', range(1, 1 + len(df)))
        
        # save dataframe to csv file
        df.to_csv(output_file, index=False)
        
        print('Player Transformation Complete: ' + output_file + ' created.')
        
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
                df2['dim_player_id'] = get_value_by_column(df_players, 'account_id', int(u), 'dim_player_id')
                
                df = pd.concat([df2, df])

        current_date = datetime.now()
        df['effective_from_date'] = pd.Timestamp(current_date)
        
        # save dataframe to csv file
        df.to_csv(output_file, index=False)
        
        print('Match Transformation Complete: ' + output_file + ' created.')
        
        
    
          
base_file_path = 'C:/Work/Python/Dota/dota_project/'
data_folder = base_file_path + 'Data/'
delta_folder = base_file_path + 'Delta/'
staging_folder = base_file_path + 'Staging/'
tables_folder = base_file_path + 'Tables/'
base_url = 'https://liquipedia.net/dota2/'
player_file_prefix = 'players_sorted_by_country'

# get today's date
date1 = datetime.today().strftime('%Y%m%d')  # use ('%Y%m%d') when live so that it only loads once per day


# create folders if they don't exist
pathlib.Path(staging_folder + 'pro_players/').mkdir(parents=True, exist_ok=True)
pathlib.Path(staging_folder + 'recent_matches/').mkdir(parents=True, exist_ok=True)
pathlib.Path(delta_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(tables_folder).mkdir(parents=True, exist_ok=True)



open_dota = OpenDota(data_folder, delta_folder, staging_folder, tables_folder, date1)

# open_dota.load_players()
# open_dota.load_matches() # takes fucking ages
open_dota.transform_players() 
open_dota.transform_matches()

