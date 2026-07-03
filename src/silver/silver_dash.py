import pandas as pd
from pathlib import Path
import numpy as np
import os
import json


BASE_DIR = Path(__file__).resolve().parent.parent
# print(BASE_DIR)
DOORDASH_BRONZE_PATH = BASE_DIR / '..' / 'data' / 'bronze' / 'dasher_delivery_information.csv'


# convert the dates column to datetime format
# dasher_data['ORDER_CREATED_TIME'] = pd.to_datetime(dasher_data['created_at'])


def drop_unwanted_columns(df):
    """
    Drops specific unwanted columns from the DataFrame."""
    
    columns = ['ACTUAL_PICKUP_TIME', 'ACTUAL_DELIVERY_TIME', "ORDER_STATUS"]
    
    df = df.drop(columns =columns, axis=1)
    
    
    return df



def standardize_text_column(df):
    """
    Converts a DataFrame columns to lowercase and replaces spaces with underscores.
    """
    for column_name in df.columns:
        new_column_name = column_name.lower().replace(' ', '_')
        
        df = df.rename(columns={column_name: new_column_name})
    
    
    return df



def convert_to_datetime_format(df):
    """
    Converts specific columns in the DataFrame to datetime format.
    """
    datetime_columns = ['order_created']
    for column in datetime_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column])
    return df



def seperate_date_time(df):
    """
    Separates date and time from datetime columns into distinct columns.
    """
    column = 'order_created'
    
    # print("column type", df[column].dtype)
    df[f'{column}_date'] = df[column].dt.date
    df[f'{column}_time'] = df[column].dt.time
    return df



def change_column_name(df):
    """
    Changes specific column names in the DataFrame."""
    
    df = df.rename(columns={'order_created_time': 'order_created'})
    
    return df



def round_to_nearest_hour(df, column_name):
    """
    Rounds the datetime values in the specified column to the nearest hour.
    """
    if column_name in df.columns:
        df[column_name] = df[column_name].dt.round('H')
    return df



# def clean_store_names(df):
#     """
#     Cleans the store name column by removing trailing IDs, data in parentheses, 
#     and extra spaces, then standardizes the remaining text.
#     Assumes the column has been standardized to 'store_name'.
#     """
#     column_name = 'store_name'
    
#     # --- Step 1: Remove parentheses and content within them ---
#     # Example: 'Lazeez (Guelph - L059)' -> 'Lazeez '
#     # Regex: \s?\(.*?\)\s? matches optional space, parenthesis, any content, closing parenthesis, optional space
#     df[column_name] = df[column_name].str.replace(r'\s?\(.*?\)\s?', ' ', regex=True)
    
#     # --- Step 2: Remove trailing numbers/IDs (e.g., 'Subway 23867-0' -> 'Subway') ---
#     # Regex: \s?\d+[\s\-\d]*$ matches optional space, one or more digits, 
#     #        followed by any number of spaces or hyphens, anchored to the end of the string.
#     df[column_name] = df[column_name].str.replace(r'\s?\d+[\s\-\d]*$', '', regex=True)

#     # --- Step 3: Remove trailing non-alphabetic characters (e.g., numbers, hyphens, etc.) ---
#     # This is often needed after Step 2 if IDs are complex.c
#     df[column_name] = df[column_name].str.strip().str.rstrip('-').str.rstrip()
    
    
#     # --- Step 4: Final Standardization ---
#     # Convert to lowercase and replace remaining multiple spaces with a single underscore
#     # df[column_name] = df[column_name].str.lower()
#     df[column_name] = df[column_name].str.lower().str.split(" - ").str[0].str.strip()
    
    
#     return df

def clean_store_name(df):
    if 'STORE_NAME' in df.columns:
       
       
       # New column order_type
        df['order_type'] = np.nan

        #cleaning rules

        # Walmart Rules
        cond_walmart_grocery = df['STORE_NAME'].str.contains('Walmart Grocery', case=False, na=False)
        df.loc[cond_walmart_grocery, 'STORE_NAME'] = 'Walmart Grocery'
        df.loc[cond_walmart_grocery, 'order_type'] = 'Pickup'

        cond_wm_1199 = df['STORE_NAME'].str.contains('Walmart Marketplace - EN - 1199', case=False, na=False)
        df.loc[cond_wm_1199, 'STORE_NAME'] = 'Walmart Stone'
        df.loc[cond_wm_1199, 'order_type'] = 'Shop & Deliver'

        cond_wm_3144 = df['STORE_NAME'].str.contains('Walmart Marketplace - EN - 3144', case=False, na=False)
        df.loc[cond_wm_3144, 'STORE_NAME'] = 'Walmart Woodlawn'
        df.loc[cond_wm_3144, 'order_type'] = 'Shop & Deliver'

        cond_sfs_1199 = df['STORE_NAME'].str.contains('SFS1199 - Walmart DFS stone', case=False, na=False)
        df.loc[cond_sfs_1199, 'STORE_NAME'] = 'SFS1L99 - Walmart DFS stone'
        df.loc[cond_sfs_1199, 'order_type'] = 'Pickup'

        cond_sfs_3144 = df['STORE_NAME'].str.contains('SFS3144 - walmart DFS woodlawn', case=False, na=False)
        df.loc[cond_sfs_3144, 'STORE_NAME'] = 'SFS3144 - walmart DFS woodlawn'
        df.loc[cond_sfs_3144, 'order_type'] = 'Pickup'

        # McDonald's Rules (using Regex)
        mcd_prefixes = ['40219-', '40281-', '4559-', '12164-', '40830-']
        cond_mcd = df['STORE_NAME'].str.contains(r'^(?:' + '|'.join(mcd_prefixes) + ')', na=False, regex=True)
        location = df.loc[cond_mcd, 'STORE_NAME'].str.extract(r'^\d+-([\w ]+) -', expand=False).str.strip()
        df.loc[cond_mcd, 'STORE_NAME'] = 'McDonalds ' + location
        df.loc[cond_mcd, 'order_type'] = 'Pickup'

        # Dairy Queen Rule
        # Find 'dq' OR 'dairy queen' using regex
        cond_dq = df['STORE_NAME'].str.contains('dq|dairy queen', case=False, na=False, regex=True)
        df.loc[cond_dq, 'STORE_NAME'] = 'Dairy Queen'
        df.loc[cond_dq, 'order_type'] = 'Pickup'

        # Grocery & Pharmacy Rules
        cond_shoppers = df['STORE_NAME'].str.contains('Shoppers', case=False, na=False)
        df.loc[cond_shoppers, 'STORE_NAME'] = 'Shoppers Drug Mart'
        df.loc[cond_shoppers, 'order_type'] = 'Shop & Deliver'

        cond_rexall = df['STORE_NAME'].str.contains('Rexall', case=False, na=False)
        df.loc[cond_rexall, 'STORE_NAME'] = 'Rexall'
        df.loc[cond_rexall, 'order_type'] = 'Shop & Deliver'

        cond_metro = df['STORE_NAME'].str.contains('Metro', case=False, na=False)
        df.loc[cond_metro, 'STORE_NAME'] = 'Metro'
        df.loc[cond_metro, 'order_type'] = 'Shop & Deliver'

        cond_nofrills = df['STORE_NAME'].str.contains('Nofrills|Shannon', case=False, na=False)
        df.loc[cond_nofrills, 'STORE_NAME'] = 'NoFrills'
        df.loc[cond_nofrills, 'order_type'] = 'Shop & Deliver'
        # print(df[cond_nofrills])

        # IMPORTANT: Zehrs rule must run *before* Loblaws rule
        cond_zehrs = df['STORE_NAME'].str.contains('Zehrs', case=False, na=False)
        df.loc[cond_zehrs, 'STORE_NAME'] = 'Zehrs'
        df.loc[cond_zehrs, 'order_type'] = 'Shop & Deliver'

        cond_food_basics = df['STORE_NAME'].str.contains('Food Basics', case=False, na=False)
        df.loc[cond_food_basics, 'STORE_NAME'] = 'Food Basics'
        df.loc[cond_food_basics, 'order_type'] = 'Shop & Deliver'

        # Loblaws/Zehrs PC Rules (run *after* Zehrs)
        cond_loblaws = df['STORE_NAME'].str.contains('LCL Drive|Loblaws', case=False, na=False, regex=True)
        df.loc[cond_loblaws, 'STORE_NAME'] = 'Zehrs PC'
        df.loc[cond_loblaws, 'order_type'] = 'Pickup'

        # Other Retailer Rules
        cond_caro = df['STORE_NAME'].str.contains('Caro leveillee', case=False, na=False)
        df.loc[cond_caro, 'STORE_NAME'] = 'Tim Hortons'
        df.loc[cond_caro, 'order_type'] = 'Pickup'

        cond_staples = df['STORE_NAME'].str.contains('Staples', case=False, na=False)
        df.loc[cond_staples, 'STORE_NAME'] = 'Staples'
        df.loc[cond_staples, 'order_type'] = 'Shop & Deliver'

        cond_michaels = df['STORE_NAME'].str.contains('Michaels', case=False, na=False)
        df.loc[cond_michaels, 'STORE_NAME'] = 'Michaels'
        df.loc[cond_michaels, 'order_type'] = 'Shop & Deliver'

        cond_home_depot = df['STORE_NAME'].str.contains('GUELPH - 7142 EXP - home depot', case=False, na=False)
        df.loc[cond_home_depot, 'STORE_NAME'] = 'Home Depot'
        df.loc[cond_home_depot, 'order_type'] = 'Pickup'

        cond_lcbo = df['STORE_NAME'].str.contains('LCBO', case=False, na=False)
        df.loc[cond_lcbo, 'order_type'] = 'Shop & Deliver'

        cond_beer = df['STORE_NAME'].str.contains('Beer Store', case=False, na=False)
        df.loc[cond_beer, 'STORE_NAME'] = 'Beer Store'
        df.loc[cond_beer, 'order_type'] = 'Pickup'

        # --- 4. Fill in the Default Value ---
        # Any row where 'order_type' is Null set to 'Pickup'
        df['order_type'] = df['order_type'].fillna('Pickup')

    
        


    else:
        print("The 'STORE_NAME' column does not exist in the loaded DataFrame.")
        
    
    return df


def create_day_of_week_features(df):
    """
    Creates the 'day_of_week' name and 'is_weekend' flag from the order date.
    Assumes 'order_created_date' exists.
    """
    date_col = 'order_created_date'
    
    # --- Step 1: Ensure the date column is a datetime type ---
    # Since the previous step converts this to a Python 'date' object, 
    # we convert it back to a Pandas datetime object temporarily to use .dt accessor.
    df['temp_date'] = pd.to_datetime(df[date_col])

    # --- Step 2: Create Day of Week Name ---
    # .dt.day_name() returns 'Monday', 'Tuesday', etc.
    df['day_of_week'] = df['temp_date'].dt.day_name()
    
    # --- Step 3: Create Is Weekend Flag (Feature Engineering) ---
    # Use the day name to quickly categorize weekend vs. weekday.
    df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])
    
    # Drop the temporary column
    df = df.drop(columns=['temp_date'])
    
    return df


if __name__ == "__main__":
    # print(dasher_data.head(5))
    dasher_data = pd.read_csv(DOORDASH_BRONZE_PATH)
    
    dropped_columns_df = drop_unwanted_columns(dasher_data)
    
    # print("unwanted",dropped_columns_df.columns)
    cleaned_store_names_df = clean_store_name(dropped_columns_df)
    # print('dasher', dasher_data.columns)
    
    # print('dropped',dropped_columns_df.columns)
    
    standard_column = standardize_text_column(cleaned_store_names_df)
    
    # print(standard_column.columns)
    # print('standard',standard_column.columns)
    changed_columns_df = change_column_name(standard_column)
    
    # print('changed',changed_columns_df.columns)
    
    converted_df = convert_to_datetime_format(changed_columns_df)
    
    # print('converted',converted_df.columns)
    rounded_to_nearest_hour_df = round_to_nearest_hour(converted_df, 'order_created')
    
    seperated_date_time = seperate_date_time(rounded_to_nearest_hour_df)
    # print(converted_df.columns)
    
    seperated_date_time = seperated_date_time.drop(columns=['order_created'], axis=1)
    
    Final_df = create_day_of_week_features(seperated_date_time)
    
    SILVER_FILE_PATH = BASE_DIR / '..' / 'data' / 'silver' / 'dasher_delivery_information_silver.csv'
        
    Final_df.to_csv(SILVER_FILE_PATH, index=False)

    # print(seperated_date_time.head(50))
    # print(seperated_date_time.columns)
