import polars as pl
import requests
from pathlib import Path
from datetime import datetime, timedelta

# Define input and output directories
input_folder = Path("cities")  # Folder containing original CSV files
api_data_folder = Path("api_fetched_data")  # Folder to store API responses
output_folder = Path("updated_cities")  # Folder to save updated CSVs

# Create folders if they don't exist
api_data_folder.mkdir(exist_ok=True)
output_folder.mkdir(exist_ok=True)

# Visual Crossing API Key
API_KEY = "U7CZCVG2NYX6JDE6GM988A8VS"
MAX_DAYS = [10, 5, 1]  # List of decreasing day limits


def fetch_weather(latitude, longitude, start_date, end_date):
    """Fetch weather data from Visual Crossing API for given coordinates and date range."""
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{start_date}/{end_date}?unitGroup=metric&key={API_KEY}&contentType=json"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️ API Error ({response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error fetching weather data: {e}")
        return None


def split_date_range(start_date, end_date, max_days):
    """Split the date range into smaller chunks based on API limits."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    date_ranges = []
    while start <= end:
        chunk_end = min(start + timedelta(days=max_days - 1), end)
        date_ranges.append((start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        start = chunk_end + timedelta(days=1)

    return date_ranges


# Process each CSV file
for file in input_folder.glob("*.csv"):
    print(f"📂 Processing: {file.name}")

    # Read CSV file
    df = pl.read_csv(file)
    
    # Ensure required columns exist
    required_columns = {"latitude", "longitude", "observed_date"}
    if not required_columns.issubset(df.columns):
        print(f"⚠️ Skipping {file.name}: Missing required columns {required_columns}")
        continue

    # Convert observed_date column to string format (YYYY-MM-DD)
    df = df.with_columns(df["observed_date"].cast(pl.Utf8))

    # Get latitude, longitude, and date range
    latitude = df["latitude"][0]  # Assuming all rows have the same latitude & longitude
    longitude = df["longitude"][0]
    start_date = df["observed_date"].min()
    end_date = df["observed_date"].max()

    print(f"🌍 Fetching weather for ({latitude}, {longitude}) from {start_date} to {end_date}")

    all_weather_data = []
    current_start = start_date

    while current_start <= end_date:
        success = False

        # Try different query sizes from MAX_DAYS list
        for days in MAX_DAYS:
            current_end = (datetime.strptime(current_start, "%Y-%m-%d") + timedelta(days=days - 1)).strftime("%Y-%m-%d")
            if current_end > end_date:
                current_end = end_date

            print(f"🔹 Trying {days} days: {current_start} to {current_end}")

            weather_info = fetch_weather(latitude, longitude, current_start, current_end)

            if weather_info and "days" in weather_info:
                # Extract relevant weather details
                for day in weather_info["days"]:
                    all_weather_data.append({
                        "observed_date": day["datetime"],
                        "temp": day.get("temp"),
                        "humidity": day.get("humidity"),
                        "windspeed": day.get("windspeed"),
                        "dew": day.get("dew"),
                        "precip": day.get("precip"),
                        "snow": day.get("snow"),
                    })

                current_start = (datetime.strptime(current_end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                success = True
                break  # Exit the loop when a successful fetch occurs

        if not success:
            print(f"❌ Unable to fetch data for {current_start}. Skipping...")
            break

    if not all_weather_data:
        print(f"❌ No weather data available for {file.name}, skipping update.")
        continue

    # Convert API response to DataFrame
    weather_df = pl.DataFrame(all_weather_data)

    # Save API fetched data separately
    api_file = api_data_folder / file.name
    weather_df.write_csv(api_file)
    print(f"✅ Saved API data: {api_file}")

    # Merge with original data based on observed_date
    updated_df = df.join(weather_df, on="observed_date", how="left")

    # Save the final updated CSV
    output_file = output_folder / file.name
    updated_df.write_csv(output_file)
    print(f"✅ Updated and saved: {output_file}")

print("🎉 Processing complete!")
