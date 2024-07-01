import matplotlib.pyplot as plt
import pandas as pd
import json
import glob
from support import get_value_by_column



    
base_file_path = 'D:/General Storage/Python/Liquipedia Data Grab/dota_project/'         
# base_file_path = 'C:/Work/Python/Dota/dota_project/'
tables_folder = base_file_path + 'Tables/'
df = pd.read_csv(tables_folder + 'fact_matches.csv')


df_players = pd.read_csv(tables_folder + 'dim_players.csv')

result = pd.merge(df, df_players, on='account_id', how='inner')
# print(result)



# Filter DataFrame where 'group' equals 'B'
# filtered_df = result[result['country_code'] == 'B']

# Count distinct rows based on all columns
# distinct_count = filtered_df.drop_duplicates().shape[0]

distinct_group_counts = result['match_id'].nunique()
distinct_group_counts = result['team_name'].nunique()
distinct_group_counts = result['country_code'].nunique()
print(distinct_group_counts)
grouped_counts = result.groupby('country_code').size().reset_index(name='counts')
print(grouped_counts)


# # Plotting the data
# plt.figure(figsize=(8, 6))
# plt.bar(grouped_counts['team_name'], grouped_counts['counts'], color='skyblue')
# plt.xlabel('Account_id')
# plt.ylabel('Counts')
# plt.title('Counts of Rows Grouped by Account_id')
# plt.show()

