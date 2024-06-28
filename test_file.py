# import pandas as pd
# import json

# # Create the DataFrame with the provided data
# data = {
#     'item_name': ['blink', 'overwhelming blink'],
#     'attrib': [
#         '[{"key":"blink_range","value":"1200"},{"key":"blink_damage_cooldown","value":"3.0"},{"key":"blink_range_clamp","value":"960"}]',
#         '[{"key":"blink_range","value":"1200"},{"key":"blink_damage_cooldown","value":"3.0"},{"key":"blink_range_clamp","value":"960"},{"key":"bonus_strength","display":"+ {value} Strength","value":"25"},{"key":"radius","value":"800"},{"key":"movement_slow","value":"50"},{"key":"attack_slow","value":"50"},{"key":"duration","value":"6"},{"key":"damage_base","value":"100"},{"key":"damage_pct_instant","value":"50"},{"key":"damage_pct_over_time","value":"100"}]'
#     ]
# }

# df = pd.DataFrame(data)

# # Step 1: Convert JSON strings to lists of dictionaries
# df['attrib'] = df['attrib'].apply(json.loads)

# # Step 2: Flatten the JSON arrays
# df_exploded = df.explode('attrib')

# # Step 3: Normalize the JSON objects into separate columns
# df_flattened = pd.concat([df_exploded.drop(columns=['attrib']), df_exploded['attrib'].apply(pd.Series)], axis=1)

# # Display the resulting DataFrame
# print(df_flattened)

# df = {
#     'item_name':['blink','overwhelming blink'],
#     'attrib':['[{"key":"blink_range","value":"1200"},{"key":"blink_damage_cooldown","value":"3.0"},{"key":"blink_range_clamp","value":"960"}]','[{"key":"blink_range","value":"1200"},{"key":"blink_damage_cooldown","value":"3.0"},{"key":"blink_range_clamp","value":"960"},{"key":"bonus_strength","display":"+ {value} Strength","value":"25"},{"key":"radius","value":"800"},{"key":"movement_slow","value":"50"},{"key":"attack_slow","value":"50"},{"key":"duration","value":"6"},{"key":"damage_base","value":"100"},{"key":"damage_pct_instant","value":"50"},{"key":"damage_pct_over_time","value":"100"}]']
# }
