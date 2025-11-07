import pandas as pd
from pathlib import Path
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



def clean_store_names(df):
    """
    Cleans the store name column by removing trailing IDs, data in parentheses, 
    and extra spaces, then standardizes the remaining text.
    Assumes the column has been standardized to 'store_name'.
    """
    column_name = 'store_name'
    
    # --- Step 1: Remove parentheses and content within them ---
    # Example: 'Lazeez (Guelph - L059)' -> 'Lazeez '
    # Regex: \s?\(.*?\)\s? matches optional space, parenthesis, any content, closing parenthesis, optional space
    df[column_name] = df[column_name].str.replace(r'\s?\(.*?\)\s?', ' ', regex=True)
    
    # --- Step 2: Remove trailing numbers/IDs (e.g., 'Subway 23867-0' -> 'Subway') ---
    # Regex: \s?\d+[\s\-\d]*$ matches optional space, one or more digits, 
    #        followed by any number of spaces or hyphens, anchored to the end of the string.
    df[column_name] = df[column_name].str.replace(r'\s?\d+[\s\-\d]*$', '', regex=True)

    # --- Step 3: Remove trailing non-alphabetic characters (e.g., numbers, hyphens, etc.) ---
    # This is often needed after Step 2 if IDs are complex.c
    df[column_name] = df[column_name].str.strip().str.rstrip('-').str.rstrip()
    
    
    # --- Step 4: Final Standardization ---
    # Convert to lowercase and replace remaining multiple spaces with a single underscore
    # df[column_name] = df[column_name].str.lower()
    df[column_name] = df[column_name].str.lower().str.split(" - ").str[0].str.strip()
    
    
    return df



if __name__ == "__main__":
    # print(dasher_data.head(5))
    dasher_data = pd.read_csv(DOORDASH_BRONZE_PATH)
    
    # print('dasher', dasher_data.columns)
    
    dropped_columns_df = drop_unwanted_columns(dasher_data)
    # print('dropped',dropped_columns_df.columns)
    
    standard_column = standardize_text_column(dropped_columns_df)
    
    # print('standard',standard_column.columns)
    changed_columns_df = change_column_name(standard_column)
    
    # print('changed',changed_columns_df.columns)
    
    converted_df = convert_to_datetime_format(changed_columns_df)
    
    # print('converted',converted_df.columns)
    rounded_to_nearest_hour_df = round_to_nearest_hour(converted_df, 'order_created')
    
    seperated_date_time = seperate_date_time(rounded_to_nearest_hour_df)
    # print(converted_df.columns)
    
    seperated_date_time = seperated_date_time.drop(columns=['order_created'], axis=1)
    

    cleaned_store_names_df = clean_store_names(seperated_date_time)
    print(cleaned_store_names_df.head(20))
