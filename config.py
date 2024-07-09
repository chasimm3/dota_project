import os
from datetime import datetime, timedelta

# BASE PARAMETERS, DO NOT CHANGE
# Get the file path of the python script
base_file_path = os.path.dirname(__file__).replace('\\','/') + '/'

print('Base File Path: ' + base_file_path) 

# get today's date
date1 = datetime.today().strftime('%Y%m%d')  # use ('%Y%m%d') when live so that it only loads once per day

# the base url of the openDota API
base_url = 'https://api.opendota.com/api/'


# BELOW PARAMETERS CAN BE ALTERED AS DESIRED

# the folder in which the staging json files will land
staging_folder = base_file_path + 'Staging/'
# the folder in while the star schema model will land
tables_folder = base_file_path + 'Tables/'
# the desired output file type of the star schema
output_file_type = 'xlsx'
# if xlsx if the desired output type, this will determine whether the tables are loaded
# to individual files or whether they will be loaded as sheets into the same file
output_file_excel_single_file = True