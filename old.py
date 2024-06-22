import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from support import make_api_request, get_value_by_column, DataManipulation


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
                                                               




base_file_path = 'C:/Work/Python/Dota/dota_project/'
data_folder = base_file_path + 'Data/'
delta_folder = base_file_path + 'Delta/'
staging_folder = base_file_path + 'Staging/'
tables_folder = base_file_path + 'Tables/'
base_url = 'https://liquipedia.net/dota2/'
player_file_prefix = 'players_sorted_by_country'

# get today's date
date1 = datetime.today().strftime('%Y%m%d')  # use ('%Y%m%d') when live so that it only loads once per day



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