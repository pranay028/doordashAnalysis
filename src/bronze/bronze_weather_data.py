import requests
import os
from pathlib import Path
import json
import pandas as pd
from dotenv import load_dotenv
from utils import get_december_to_now_ranges

load_dotenv()


def get_historical_weather_one_day(api_key, location, start_date):
    url = f'https://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={start_date}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()
        
    
def get_historcal_weather_date_range(api_key, location, start_date, end_date):
    url = f'https://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={start_date}&end_dt={end_date}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()
        
def get_daily_data(weather_data):
    daily_data = []
    for day in weather_data.get('forecast', {}).get('forecastday', [])[0:10]:
        day_info = {
            'date': day['date'],
            'max_temp_c': day['day']['maxtemp_c'],
            'min_temp_c': day['day']['mintemp_c'],
            'avg_temp_c': day['day']['avgtemp_c'],
            'total_precip_mm': day['day']['totalprecip_mm'],
            'condition': day['day']['condition']['text'],
            'sunrise': day['astro']['sunrise'],
            'sunset': day['astro']['sunset'],
        }
        daily_data.append(day_info)
    
        
    return daily_data

def get_hourly_data(weather_data):
    hourly_data = []
    for day in weather_data.get('forecast', {}).get('forecastday', []):
        for hour in day.get('hour', [])[0:6]:
            hour_info = {
                'time': hour['time'],
                'temp_c': hour['temp_c'],
                'is_day': hour['is_day'],
                'condition': hour['condition']['text'],
                'wind_kph': hour['wind_kph'],
                'humidity': hour['humidity'],
                'precip_mm': hour['precip_mm'],
                'snow_cm': hour['snow_cm'],
                'uv': hour['uv'],
                'feel_like_c': hour['feelslike_c'],
                'chance_of_rain': hour.get('chance_of_rain', 0),
                'chance_of_snow': hour.get('chance_of_snow', 0),
                
                
            }
            hourly_data.append(hour_info)
        
            
    return hourly_data


if __name__ == "__main__":
    API_KEY = os.environ.get('WEATHER_API_KEY')
    LOCATION = os.environ.get('LOCATION')
    # start_date = '2024-11-10'  # Example start date
    # end_date    = '2024-11-11'    # Example end date
    # 1. Generate the ranges
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    WEATHER_BRONZE_DIR = PROJECT_ROOT / 'data' / 'bronze'
    
    # print(f"File path: {file_path}")
    monthly_ranges = get_december_to_now_ranges()
    print(f"Monthly Ranges: {monthly_ranges}")
    # 2. Loop through the ranges and call the API
    # for start_date, end_date in monthly_ranges:
    # # # Build your API URL using the date variables
        
    #     weather_data = get_historcal_weather_date_range(API_KEY, LOCATION, start_date, end_date)
    #     daily_data = get_daily_data(weather_data)
    #     hourly_data = get_hourly_data(weather_data)
    
    # # ... process and save JSON ...
    # print(f"Successfully processed data from {start_date} to {end_date}")
    # daily_file_path = WEATHER_BRONZE_DIR / f'weather_data_daily_2025-10-01_to_2025-10-31.json'
    # with open(daily_file_path, 'w') as f:
    #     json.dump(daily_data, f, indent=4)
        
    # horuly_file_path = f'weather_data_hourly_{start_date}_to_{end_date}.json'
    # full_hourly_file_path = WEATHER_BRONZE_DIR / horuly_file_path
    # with open(full_hourly_file_path, 'w') as f:
    #     json.dump(hourly_data, f, indent=4)
    # daily_file_path = WEATHER_BRONZE_DIR / f'test_weather_data_daily_2025-10-01_to_2025-10-31.json'
    # with open(daily_file_path, 'w') as f:
    #     json.dump(daily_data, f, indent=4)
        
    # horuly_file_path = f'test_weather_data_hourly_{start_date}_to_{end_date}.json'
    # full_hourly_file_path = WEATHER_BRONZE_DIR / horuly_file_path
    # with open(full_hourly_file_path, 'w') as f:
    #     json.dump(hourly_data, f, indent=4)