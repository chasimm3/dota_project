import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser
import json
import glob
import os
import time
import pathlib

class Dota2Players():
    def __init__(self, base_url, date1, data_folder, delta_folder, file_prefix):
        self.base_url = base_url
        self.date1 = date1
        self.data_folder = data_folder
        self.delta_folder = delta_folder
        self.file_prefix = file_prefix
        self.df_new = []
        
    def write_players_to_file(self):
        url = self.base_url + "Players_(all)"
        output_file = self.data_folder + self.file_prefix + '_' + self.date1 + '.csv'
        # Send a GET request to fetch the HTML content of the page
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')

        # Initialize an empty list to store dataframes
        dataframes = []

        # Iterate through the content to find country headers and tables
        for header in soup.find_all(['h3']):
            country = header.get_text(strip=True)
            table = header.find_next('table', class_='wikitable')
            
            if table:
                # Convert the table to a pandas dataframe
                df = pd.read_html(str(table))[0]
                
                df = df[0:]  # Remove the first header row
                
                df.columns = df.columns.droplevel()
                
                # Add a 'Country' column
                df['Country'] = country
                # Drop the "Links" column as it's not needed
                df.drop(df.columns[3], axis=1, inplace=True)
                # Append the dataframe to the list
                dataframes.append(df)
                
        # Concatenate all dataframes into one
        all_data = pd.concat(dataframes, ignore_index=True)

        # Sort the dataframe by the 'Country' column
        sorted_data = all_data.sort_values(by=['Country', 'ID'])

        # print(list(sorted_data.columns))

        # Save the sorted dataframe to a CSV file
        sorted_data.to_csv(output_file, index=False)

        self.df_new = sorted_data
        
        print('Data has been scraped and saved to ' + output_file)
        
        return self.df_new

        
class Dota2Teams():
    def __init__(self):
        # URL of the page to scrape
        self.base_url = 'https://liquipedia.net/dota2/api.php?'
        self.date = datetime.today().strftime('%Y%m%d%H%M%S')  # use ('%Y%m%d') when live so that it only loads once per day
        self.headers = ({'User-Agent':'bisquit-project'},{'Accept-Encoding':'gzip'})
        self.params = "&action=parse&format=json"
        self.data_folder = 'Data/'
        self.delta_folder = 'Deltas/'
        
    def write_teams_to_file(self):
        output_file = self.data_folder + 'teams_sorted_by_region_' + self.date + '.csv'
        
        # sent API call to liquipedia teams page and parse the html output
        url = 'https://liquipedia.net/dota2/Portal:Teams'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        
        with open(output_file, "w") as f:
            f.write('Team, Region, Team Status \n')
        # find the h2 headers that are not in the contents sidebar
        for header in soup.find_all(['h2']):
            for span in header:
                if span.find("mw-headline") != -1:
                    for id in span:
                        if id != "Contents" and len(id) > 1:
                            team_status = id
            
            # for each header, find the sibling regions, then find the sibling teams
            for sibling in header.find_next_siblings():
                if sibling.name == "h2":
                    break
                for sibling2 in sibling.find_all("h3"):
                    region_name = sibling2("span")[0]   
                
                    for sibling3 in sibling2.find_next("div",{"class":"panel-box-body"}):
                        for sibling4 in sibling3.find_all('span', {'class':'team-template-text'}):
                            team_name = sibling4("a", href=True)[0]
                            file_content = team_name['title'] + ',' + region_name['id'] + ',' + team_status + '\n'
                            with open(output_file, "a") as f:
                                f.write(file_content)
                                
                                
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
        
    def compare_data(self, file_prefix, df_new, date):
        
        self.file_prefix = file_prefix
        self.df_new = df_new
        self.date = date
        
        # initiate delta file name
        self.delta_file = self.delta_folder + self.file_prefix + '_deltas_' + self.date + '.csv'
        
        # get different values between dataframes
        df_comp = pd.merge(self.df_last, self.df_new, how='outer', indicator='Change Type')
        df_comp = df_comp.loc[df_comp['Change Type'] != 'both']    
        
        # change the text of the indicator column "Change Type" to New or Deleted
        df_comp['Change Type'] = df_comp['Change Type'].str.replace('left_only', 'Deleted').str.replace('right_only', 'New')
        
        # output the delta to the delta folder
        df_comp.to_csv(self.delta_file, index=False)    

class OpenDota():
    def __init__(self, data_folder, delta_folder, staging_folder, date1):
        
        # initialise variables
        self.base_url = 'https://api.opendota.com/api/'
        self.data_folder = data_folder
        self.delta_folder = delta_folder
        self.staging_folder = staging_folder
        
        
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
        df_columns = ('account_id', 'last_match_time')
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
                
            
    def make_api_request(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status() # Raise an error for bad status code
            return response.json()
        except requests.RequestException as e:
            return {'error': str(e)}

        
    def transform_matches(self):
        
        # open the file
        with open(self.players_file_path, "r") as json_file:
            data = json.load(json_file)
            
        # initialise dataframe
        df_columns = ('player_name', 'account_id', 'team_name', 'last_match_time')
        self.df_players = pd.DataFrame(columns=(df_columns))
    
        i = 0
        # for each item in json array, get player name, player steam id 
        # and player team and append them to the dataframe
        for item in data:
            if not (item['name'] is None):
                player_name = item['name']
            else:
                player_name = ''
            if not (item['account_id'] is None):
                account_id = str(item['account_id'])
            else:
                account_id = ''
            if not (item['team_name'] is None):
                team_name = item['team_name']
            else:
                team_name = ''
            if not (item['last_match_time'] is None):
                last_match_time_str = str(item['last_match_time'])
            else:
                last_match_time_str = ''
                
            new_row = pd.Series({'player_name':player_name, 'account_id':account_id, 'team_name':team_name, 'last_match_time':last_match_time_str})
                        
            
            #print(type(item['last_match_time']))
            if not (item['last_match_time'] is None and type(last_match_time_str != None)):
                last_match_time = parser.parse(last_match_time_str)
                current_date = datetime.now(last_match_time.tzinfo)                        
                
                date_difference = current_date - last_match_time
                if (date_difference < timedelta(hours=5)):# and player_name == 'SaberLight':
                    recent_matches_url = self.base_url + 'players/' + str(account_id) + '/recentMatches/'          
                    print(str(i) + ' - Player: ' + str(account_id) + ' last played on :' + last_match_time_str )         
                    i +=1
                    
                    # self.matches = requests.get(recent_matches_url).json()

                    # with open(self.data_folder + self.output_file, "a") as f:
                    #     json.dump(self.matches, f, ensure_ascii=False, indent=4)
                    
                    # time.sleep(1)
            
            # append new row to end of dataframe
            self.df_players.loc[len(self.df_players)] = new_row
        # sorted by last_match_time
        self.df_players = self.df_players.sort_values('last_match_time')
        self.df_players = self.df_players.reset_index(drop=True)
        # load to csv
        self.df_players.to_csv(self.data_folder + 'pro_players.csv')
        
        
        for index, row in self.df_players.iterrows():
            if row['player_name'] == 'Newsham':
                recent_matches_url = self.base_url + 'players/' + str(row['account_id']) + '/recentMatches/'              
                print(recent_matches_url)
            

          
          

data_folder = 'Data/'
delta_folder ='Delta/'
staging_folder = 'Staging/'
base_url = 'https://liquipedia.net/dota2/'
# get today's date
date1 = datetime.today().strftime('%Y%m%d')  # use ('%Y%m%d') when live so that it only loads once per day
player_file_prefix = 'players_sorted_by_country'

# create folders if they don't exist
pathlib.Path(staging_folder + 'pro_players/').mkdir(parents=True, exist_ok=True)
pathlib.Path(staging_folder + 'recent_matches/').mkdir(parents=True, exist_ok=True)
pathlib.Path(delta_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)

open_dota = OpenDota(data_folder, delta_folder, staging_folder, date1)

open_dota.load_players()
open_dota.load_matches()

# Initialise Data Manipulation class
data_man = DataManipulation(data_folder, delta_folder)



# # Initialise Dota 2 Players class
# load_players = Dota2Players(base_url, date1, data_folder, delta_folder, player_file_prefix)

# # Get the latest run from the data Folder
# data_man.get_last_run(player_file_prefix)

# # Scrap the url and load the data into a new file
# load_players.write_players_to_file()

# # compare the old and new files and load the changes into the delta folder
# data_man.compare_data(player_file_prefix, load_players.df_new, date)


# load_teams = Dota2Teams()
# load_teams.write_teams_to_file()