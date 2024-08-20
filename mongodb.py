import json
import glob
import config
import pymongo

# initialise variables
base_file_path = config.staging_folder


def load_file(col_name, data):
    # add pro_players file to players collection in mongodb
    mongo_col = config.mongo_db[col_name]
    if isinstance(data, list):
        mongo_col.insert_many(data)
    else:
        mongo_col.insert_one(data)   
        

def empty_collection(col_name):  
    # remove pro_players file from players collection in mongodb
    mongo_col = config.mongo_db[col_name]
    query_filter = {}
    result = col_name.delete_many(query_filter)


def load_folder(col_name, folder_path):
    # loop through folder to load files to collection
    mongo_col = config.mongo_db[col_name]
    filelist = glob.glob(folder_path + '*.json')
    for filename in filelist:
        with open(filename) as f:
            file_data = json.load(f)
        load_file(col_name, file_data)


load_folder("__heroes", base_file_path + 'heroes/')
load_folder("__items", base_file_path + 'constants/items/')
load_folder("__item_ids", base_file_path + 'constants/item_ids/')
load_folder("__patchnotes", base_file_path + 'constants/patchnotes/')
load_folder("__players", base_file_path + 'pro_players/')
load_folder('__recent_matches', base_file_path + 'recent_matches/')