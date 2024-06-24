import pandas as pd

# Example DataFrame
data = {
    'group': ['A', 'A', 'B', 'B', 'B', 'C', 'C', 'C', 'C'],
    'value': [10, 20, 30, 40, 50, 60, 70, 80, 90]
}

df = pd.DataFrame(data)

# Filter DataFrame where 'group' equals 'B'
filtered_df = df[df['group'] == 'B']

# Count distinct rows based on all columns
distinct_count = filtered_df.drop_duplicates().shape[0]

print(f"Distinct rows where group = 'B': {distinct_count}")
