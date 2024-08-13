import pymongo
import json
import os
import glob

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dota_project"]
mycol = mydb["__players"]

base_file_path = os.path.dirname(__file__).replace('\\','/') + '/'
players_path = base_file_path + 'staging/pro_players/'
players_file_path =  base_file_path + 'staging/pro_players/pro_players_20240812.json'
# players_file_path = base_file_path + 'staging/pro_players/pro_players_20240623.json'



def load_file(col_name, data):
    # add pro_players file to players collection in mongodb
    if isinstance(data, list):
        mycol.insert_many(data)
    else:
        mycol.insert_one(data)   
        

def empty_collection(col_name):  
    # remove pro_players file from players collection in mongodb
    query_filter = {}
    result = col_name.delete_many(query_filter)


# loop through folder to load files to collection
filelist = glob.glob(players_path + '*.json')
for filename in filelist:
    with open(filename) as f:
        file_data = json.load(f)
    load_file(mycol, file_data)
     

    
# empty_collection(mycol)