import pandas as pd
import numpy as np
import re

# Read the CSV file
data = pd.read_csv('zomato.csv')

# Drop the specified columns
columns_to_drop = ['address', 'phone']
data = data.drop(columns=columns_to_drop, axis=1)

# Convert all text columns to lowercase
data = data.applymap(lambda x: x.lower() if isinstance(x, str) else x)

# Rename columns
column_mapping = {
    'rate': 'ratings',
    'approx_cost(for two people)': 'approx_cost',
    'listed_in(type)': 'type'
}
data.rename(columns=column_mapping, inplace=True)

# Check for null values and blanks in each column
null_counts = data.isnull().sum()

# Display the null and blank counts for each column
print("Null Value Counts:")
print(null_counts)

# Checking conditions for handling null values in 'ratings' and 'votes'
newly_opened_conditions = (data['ratings'].isnull()) & (data['votes'] == "0") & (data['dish_liked'].isnull())
data.loc[newly_opened_conditions, 'ratings'] = 0.0 # assigning 0.0 as rating since these hotels are newly opened

# Apply the transformation only for rows where 'newly_opened_conditions' are true
data.loc[newly_opened_conditions, 'dish_liked'] = "not specified"

# Dropping rows with null values in 'name', 'book_table', and 'location'
data.dropna(subset=['name', 'book_table', 'location'], inplace=True)

# Drop rows where all columns have null values
data.dropna(how='all', inplace=True)

# Drop rows where 5 or more columns have null values
data.dropna(thresh=data.shape[1] - 5 + 1, inplace=True)

# Decide on duplicate handling strategy and create a cleaned DataFrame
data = data.drop_duplicates()

# Drop rows where "RATED" is present in any column
data = data[~data.apply(lambda row: row.astype(str).str.contains('RATED').any(), axis=1)]

# Function to remove special characters from a string
def remove_special_characters(text):
    return re.sub(r'[^a-zA-Z0-9\s]', '', text)

# Apply the text cleaning function to the 'name' column
data['name'] = data['name'].apply(remove_special_characters)

# Drop rows with values other than 'yes' and 'no' in 'online_order' column
data = data[data['online_order'].isin(['yes', 'no'])]

# Drop rows with values other than 'yes' and 'no' in 'book_table' column
data = data[data['book_table'].isin(['yes', 'no'])]


# Transform rest columns
def transform_rest_type(data):
    for index, row in data.iterrows():
        if pd.isnull(row['rest_type']):
            if not pd.isnull(row['dish_liked']):
                if not pd.isnull(row['cuisines']):
                    if row['type'] == 'delivery':
                        data.at[index, 'rest_type'] = 'cloud kitchen'
                    elif row['type'] == 'dine-out':
                        data.at[index, 'rest_type'] = 'casual Dining'
                    elif row['type'] == 'desserts':
                        data.at[index, 'rest_type'] = 'bakery, dessert parlour'
        elif pd.isnull(row['dish_liked']):
            data.at[index, 'dish_liked'] = 'not specified'
    return data

# Apply the transformation function
data = transform_rest_type(data.copy())

# Conditionally update 'type' column based on 'rest_type' column values
type_mapping = {
    'cafe': 'cafes',
    'casual dining': 'dine-out',
    'pub': 'pubs and bars',
    'delivery': 'delivery',
    'lounge': 'drinks & nightlife',
    'quick bites' : 'cafes',
    'microbrewery' : 'drinks & nightlife',
    'dessert parlour' : 'Dessert',
    'fine dining' : 'buffet',
    'bakery' : 'Dessert',
    'club' : 'drinks & nightlife'
}

for index, row in data[data['type'].isnull()].iterrows():
    rest_type = row['rest_type']
    if isinstance(rest_type, str):
        for keyword, new_type in type_mapping.items():
            if keyword in rest_type:
                data.at[index, 'type'] = new_type

# Final cleaning of rating column
data['ratings'] = data['ratings'].str.split('/').str[0].str.strip()
data['ratings'].fillna('0.0', inplace=True)

# Final cleaning for null values in 'approx_cost' column
data['approx_cost'] = data['approx_cost'].str.replace(',', '')  # Remove commas
mean_cost = data['approx_cost'].astype(float).mean()
data['approx_cost'].fillna(mean_cost, inplace=True)

# Final - Replacing null values in 'type' column
type_counts = data['type'].value_counts()
most_common_types = type_counts.index[:3]  # Get the 3 most common types

null_type_indices = data[data['type'].isnull()].index
for index in null_type_indices:
    data.at[index, 'type'] = np.random.choice(most_common_types)

# Final - Replacing null values in 'rest_type' column
type_counts = data['rest_type'].value_counts()
most_common_types = type_counts.index[:4]  # Get the 4 most common types

null_type_indices = data[data['rest_type'].isnull()].index
for index in null_type_indices:
    data.at[index, 'rest_type'] = np.random.choice(most_common_types)

# Fianlly check for null values in cuisines and drop
data.dropna(subset=['cuisines'], inplace= True)

# Correcting datatypes for whole data:
data['name'] = data['name'].astype(str)
data['online_order'] = data['online_order'].astype(str)
data['book_table'] = data['book_table'].astype(str)
data['ratings'] = pd.to_numeric(data['ratings'], errors='coerce')
data['ratings'] = data['ratings'].fillna(0.0).astype(float)
data['votes'] = data['votes'].astype(int)
data['location'] = data['location'].astype(str)
data['rest_type'] = data['rest_type'].astype(str)
data['dish_liked'] = data['dish_liked'].astype(str)
data['cuisines'] = data['cuisines'].astype(str)
data['approx_cost'] = data['approx_cost'].astype(float)
data['type'] = data['type'].astype(str)

# Adding an id for better management of data
data.insert(0, 'id', range(1, 1 + len(data)))

# Display null counts after transformations
null_counts_after_transformations = data.isnull().sum()
print("\nNull Value Counts After Transformations:")
print(null_counts_after_transformations)

# Display the modified DataFrame
print(data.head()) 

# Save the modified DataFrame back to a CSV file
data.to_csv('cleaned_zomato.csv', index=False)
