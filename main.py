import requests
import pandas as pd
from datetime import datetime, timedelta
from time import strftime, localtime
from dateutil import parser
from pathlib import Path
import json
import glob
import os
import time
from support import get_value_by_column, load_json_to_file, create_folder, load_csv, load_parquet, load_excel
import config



class OpenDota():
    def __init__(self, staging_folder, tables_folder, date1, output_file_type):
        
        # initialise variables
        self.base_url = 'https://api.opendota.com/api/'
        self.staging_folder = staging_folder
        self.tables_folder = tables_folder
        self.output_file_type = output_file_type
        self.date1 = date1
        
    
    def load_parent(self, url_x, folder):
        # initialize variables
        url = self.base_url + url_x
        file_path = self.staging_folder + folder + self.date1 + '.json'
        
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
        players_file_path = self.staging_folder + 'pro_players/pro_players_' + self.date1 +'.json'
        
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
                    
                    print('Loading from URL: ' + row['recent_matches_url'] + '' + str(account_id) + '_' + str(self.date1) + '.json')
                    self.matches = requests.get(row['recent_matches_url']).json()

                    with open(output_folder + str(account_id) + '_' + str(self.date1) + '.json', "w") as f:
                        json.dump(self.matches, f, ensure_ascii=False, indent=4)            
                    time.sleep(1)         
        
    def transform_dimension(self, file_path, output_file_path, dimension_name, output_file_excel_single_file):
        
        pk_name = dimension_name + '_id'
        
        list_of_files = glob.glob(file_path + '*') # * means all, if need specific format then *.csv
        latest_file = max(list_of_files, key=os.path.getctime)
        print('Loading file: ' + latest_file)
        
        with open(latest_file, "r") as json_file:
            data = json.load(json_file)
            
        # initialise dataframe of player data in the staging file
        df = pd.json_normalize(data)
        
        
        # # add effective to date column on the end
        current_date = datetime.now()
        df['load_date'] = pd.Timestamp(current_date)
        
        # insert incremental integer in column 1 
        df.insert(0, pk_name, range(1, 1 + len(df)))
        
        # load dataframe to file,
        if self.output_file_type == 'csv':
            load_csv(df, output_file_path + '.csv', '|')
        if self.output_file_type == 'parquet':
            load_parquet(df, output_file_path + '.parquet.gzip')
        if self.output_file_type == 'xlsx' and output_file_excel_single_file == False: 
            load_excel(df, output_file_path, dimension_name, False)
        if self.output_file_type == 'xlsx' and output_file_excel_single_file == True:
            load_excel(df, 'Tables/single_file', dimension_name, True)    

        
        print('Player Transformation Complete: ' + output_file_path + '.' + self.output_file_type + ' created.')
        
        
    def transform_matches(self):
        
        matches_file_path = self.staging_folder + 'recent_matches/'
        output_file_path = self.tables_folder + 'fact_matches'
        
        ## aiming to remove
        
        list_of_files = []
        # only get files that are loaded today:
        for filename in os.listdir(matches_file_path):
            file_path = os.path.join(matches_file_path, filename)
            
            # if file, get creation time and date
            if os.path.isfile(file_path):
                create_time = os.path.getctime(file_path)
                creation_date = datetime.fromtimestamp(create_time).date()
                
                # if creation_date == datetime.now().date():
                list_of_files.append(file_path)

        # initialise dataframe
        df = pd.DataFrame()
        
        
        # load players csv file to dataframe using support functions depending on output type
        df_players = pd.DataFrame()
        if self.output_file_type ==  'csv':
            players_file = self.tables_folder + 'dim_player.csv'
            df_players = pd.read_csv(players_file, header='infer', sep='|')
        if self.output_file_type == 'parquet':
            players_file = self.tables_folder + 'dim_player.parquet.gzip'
            df_players = pd.read_parquet(players_file)
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == False:
            players_file = self.tables_folder + 'dim_player.xlsx'
            df_players = pd.read_excel(players_file)
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == True:
            players_file = self.tables_folder + 'single_file.xlsx'
            df_players = pd.read_excel(players_file, sheet_name='dim_player')

        # load players csv file to dataframe using support functions depending on output type
        df_hero = pd.DataFrame()
        if self.output_file_type ==  'csv':
            hero_file = self.tables_folder + 'dim_hero.csv'
            df_hero = pd.read_csv(players_file, header='infer', sep='|')
        if self.output_file_type == 'parquet':
            hero_file = self.tables_folder + 'dim_hero.parquet.gzip'
            df_hero = pd.read_parquet(players_file)
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == False:
            hero_file = self.tables_folder + 'dim_hero.xlsx'
            df_hero = pd.read_excel(players_file)
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == True:
            hero_file = self.tables_folder + 'single_file.xlsx'
            df_hero = pd.read_excel(players_file, sheet_name='dim_hero')
           
        
        # loop through json files in staging folder
        for i in list_of_files:
            with open(i, "r") as json_file:
                match_data = json.load(json_file)
                
                # append json array items to dataframe
                df2 = pd.json_normalize(match_data)
                
                # get date from the file by reversing the string and getting the characters up until the first underscore
                date = (((i[::-1])[:((i[::-1]).find('_'))])[::-1]).replace('.json', '')
                # from the file name, get the string up until the location of the date 
                j = i[0:i.find(date)-1]
                # get the account id from the file name by reversing the string and getthing the characters up until the first underscore now that date has been removed
                account_id = (((j[::-1])[:((j[::-1]).find('_'))])[::-1]).replace('.json', '')
                
                df2['account_id'] = account_id
                
                # get the dim_player_id for the account id
                df2['fk_dim_player_id'] = get_value_by_column(df_players, 'account_id', int(account_id), 'dim_player_id')
                
                df2['load_date'] = date
                
                df = pd.concat([df2, df])

        # merge dataframe to hero dimension and pull in dim_hero_id to a new column, fk_dim_hero_id
        df = df.merge(df_hero[['dim_hero_id', 'id']],  left_on='hero_id', right_on='id', how='left').rename(columns={'dim_hero_id':'fk_dim_hero_id'}).drop(columns='id')   
        
        # save dataframe to csv file
        if self.output_file_type == 'csv':
            load_csv(df, output_file_path + '.csv', '|')
        if self.output_file_type == 'parquet':
            load_parquet(df, output_file_path + '.parquet.gzip')
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == False: 
            load_excel(df, output_file_path, 'fact_matches', False)
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == True:
            load_excel(df, 'Tables/single_file', 'fact_matches', True)    
                
        
        print('Match Transformation Complete: ' + output_file_path + ' created.')
        
    def transform_items(self, reference_folder):
        items_file_path = self.staging_folder + 'constants/items/'
        # output_file_path = self.tables_folder + 'dim_items.csv'    
        live_items_file_path = self.staging_folder + reference_folder + 'items_live.csv'
        output_file_path = self.tables_folder + 'dim_item'
        
        # establish dataframe of live items
        df_live_items = pd.read_csv(live_items_file_path)
        
        # open latest items file and write to dataframe
        list_of_files = glob.glob(items_file_path + '*') # * means all, if need specific format then *.csv
        items_latest_file = max(list_of_files, key=os.path.getctime)
        print("Starting Item Transformation. Loading data from: " + items_latest_file)
        # items_latest_file = 'D:/General Storage/Python/Liquipedia Data Grab/dota_project/Staging/constants/items/items_20240627.json'
        with open(items_latest_file, "r") as json_file:
            items_data = json.load(json_file)
        
        
        # transpose the items data to column then rename it to item_name
        df_items = pd.DataFrame(items_data).transpose().reset_index().rename(columns={"index":"item_name"})

        # join the items to the live items to remove non-live items from the dataframe
        df_items = pd.merge(df_items, df_live_items, how='inner', left_on='id', right_on='ItemID')
        

        # Extract 'ability' and 'item_name' columns from df_items, remove items without an ability array
        ability_series = df_items['abilities'].dropna()
        df_item_name = pd.DataFrame(df_items['item_name'])
        
        # for each record pivot the series to new columns
        df_ability_list = pd.concat({i: pd.DataFrame(x) for i, x in ability_series.items()})
        # join the ability list and to the item names based on the index 
        df_abilities = pd.DataFrame.join(df_ability_list.reset_index(), df_item_name.reset_index(), on='level_0').drop(columns={'level_0', 'level_1','index'}).reindex()
        
        # rename old columns and format item_name for joining
        df_abilities = df_abilities.rename(columns={'title':'ability_title','type':'ability_type','description':'ability_description'})
        df_abilities['item_name'] = df_abilities['item_name'].str.replace('_', ' ').str.lower()


        # Extract 'attrib' and 'item_name' columns from df_items, remove items without an attrib array
        attrib_series = df_items['attrib'].dropna()
        df_item_name = pd.DataFrame(df_items['item_name'])

        # for each record pivot the series to new columns
        df_attrib_list = pd.concat({i: pd.DataFrame(x) for i, x in attrib_series.items()})
        # join the attrib list and to the item names based on the index 
        df_attribs = pd.DataFrame.join(df_attrib_list.reset_index(), df_item_name.reset_index(), on='level_0').drop(columns={'level_0', 'level_1','index'}).reindex()
        
        # rename attribute columns
        df_attribs = df_attribs.rename(columns={'key':'attribute_type','value':'attribute_value','display':'attribute_display'})
             
        # remove spaces and make item_name lower case
        df_attribs['item_name'] = df_attribs['item_name'].str.replace('_', ' ').str.lower()

        # Convert both columns to lower case
        df_abilities['item_name'] = df_abilities['item_name'].str.lower()
    
        # Replace underscores with spaces in the 'item_name' column
        df_items['item_name'] = df_items['item_name'].str.replace('_', ' ').str.lower()

        # merge df_items and df_abiliites on item_name
        df_merged = pd.merge(df_items, df_abilities, how='left', left_on='item_name', right_on='item_name')
        
        # merge df_merged and df_attribs on item_name
        df_merged = pd.merge(df_merged, df_attribs, how='left', left_on='item_name', right_on='item_name').drop(columns={'abilities','attrib'})        
        
        # drop live items columns
        df_merged = df_merged.drop(columns={"ItemID",	"InternalName",	"EnglishName"})
        
        # # add effective to date column on the end
        current_date = datetime.now()
        df_merged['load_date'] = pd.Timestamp(current_date)
        
        # insert incremental integer in column 1 
        df_merged.insert(0, 'dim_item_id', range(1, 1 + len(df_merged)))
        
        
        # save dataframe to specified output file type
        if self.output_file_type == 'csv':
            load_csv(df_merged, output_file_path + '.csv', '|')
        if self.output_file_type == 'parquet':
            # convert non-parquet function columns to str
            df_merged = df_merged.astype(str)
            load_parquet(df_merged, output_file_path + '.parquet.gzip')
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == False: 
            load_excel(df_merged, output_file_path, 'dim_item', False)
        if self.output_file_type == 'xlsx' and config.output_file_excel_single_file == True:
            load_excel(df_merged, 'Tables/single_file', 'dim_item', True)    
        
        print('Item Transformation Complete: ' + output_file_path + '.' + config.output_file_type + ' created.')



# create folders if they don't exist
create_folder(config.staging_folder + 'pro_players/')
create_folder(config.staging_folder + 'recent_matches/')
create_folder(config.staging_folder + 'heroes/')
create_folder(config.staging_folder + 'constants/items/')
create_folder(config.staging_folder + 'constants/item_ids/')
create_folder(config.staging_folder + 'constants/patchnotes/')
create_folder(config.staging_folder + 'reference/')
create_folder(config.tables_folder)

# initialise class
open_dota = OpenDota(config.staging_folder, config.tables_folder, config.date1, config.output_file_type)

# # stage all data
open_dota.load_parent('heroes', 'heroes/heroes_')
open_dota.load_parent('proPlayers', 'pro_players/pro_players_')
open_dota.load_parent('constants/items', 'constants/items/items_')
open_dota.load_parent('constants/item_ids', 'constants/item_ids/item_ids_')
open_dota.load_parent('constants/patchnotes', 'constants/patchnotes/patchnotes_')
open_dota.load_matches() # takes fucking ages

# build dim_items
open_dota.transform_items('reference/')
# build dim_player
open_dota.transform_dimension(config.staging_folder + 'pro_players/', config.tables_folder + 'dim_player', 'dim_player', config.output_file_excel_single_file)
# build dim_hero
open_dota.transform_dimension(config.staging_folder + 'heroes/', config.tables_folder + 'dim_hero', 'dim_hero', config.output_file_excel_single_file)
# transform matches
open_dota.transform_matches()




