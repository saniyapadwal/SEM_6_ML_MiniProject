import openmeteo_requests
import requests_cache
import pandas as pd
import os
import time
from retry_requests import retry

# Setup Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Path to folder containing city CSV files
input_folder = "cities"
output_folder = "updated_weather_data_cities"
os.makedirs(output_folder, exist_ok=True)

# Specify the file to start processing from
start_from_file = "Bhojpur.csv"  # Change this to the desired file

# Function to fetch weather data with automatic retry on rate limit error
def fetch_weather_data(lat, lon, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_mean", "precipitation_sum", "rain_sum", "snowfall_sum", "wind_speed_10m_max"],
        "timezone": "Asia/Bangkok"
    }
    
    while True:  # Keep retrying if API rate limit is exceeded
        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]  # Process first location

            # Extract daily weather data
            daily = response.Daily()
            dates = pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )

            weather_data = {
                "date": dates,
                "temperature_2m_mean": daily.Variables(0).ValuesAsNumpy(),
                "precipitation_sum": daily.Variables(1).ValuesAsNumpy(),
                "rain_sum": daily.Variables(2).ValuesAsNumpy(),
                "snowfall_sum": daily.Variables(3).ValuesAsNumpy(),
                "wind_speed_10m_max": daily.Variables(4).ValuesAsNumpy(),
            }
            return pd.DataFrame(weather_data)

        except Exception as e:
            error_message = str(e)
            if "Minutely API request limit exceeded" in error_message:
                print("⚠️ API rate limit exceeded! Waiting for 1 minute before retrying...")
                time.sleep(60)  # Wait for 1 minute
            else:
                raise  # Raise other errors normally

# Get list of files and sort them
files = sorted([f for f in os.listdir(input_folder) if f.endswith(".csv")])

# Start processing from the specified file
start_processing = False

for file in files:
    if file == start_from_file:
        start_processing = True  # Start processing when this file is reached

    if not start_processing:
        continue  # Skip files until we reach the specified file

    file_path = os.path.join(input_folder, file)
    df = pd.read_csv(file_path)

    # Extract latitude, longitude, and observed dates
    latitude, longitude = df.iloc[0][["latitude", "longitude"]]
    df["observed_date"] = pd.to_datetime(df["observed_date"]).dt.date  # Ensure date format

    # Fetch weather data with retry mechanism
    start_date, end_date = df["observed_date"].min(), df["observed_date"].max()
    weather_df = fetch_weather_data(latitude, longitude, start_date, end_date)

    # Ensure date format in weather data
    weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date

    # Merge based on observed_date and fetched weather date
    merged_df = df.merge(weather_df, left_on="observed_date", right_on="date", how="left").drop(columns=["date"])

    # Save new CSV file
    output_file_path = os.path.join(output_folder, f"updated_{file}")
    merged_df.to_csv(output_file_path, index=False)
    print(f"✅ Updated file saved: {output_file_path}")
