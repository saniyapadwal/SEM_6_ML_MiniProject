import polars as pl
import requests
from pathlib import Path

# Define input and output directories
input_folder = Path("cities")  # Source folder for CSV files
output_folder = Path("updated_cities")  # Folder to save updated CSVs
output_folder.mkdir(exist_ok=True)  # Create folder if not exists

# Visual Crossing API Key
API_KEY = "U7CZCVG2NYX6JDE6GM988A8VS"


# Function to fetch weather data from the API
def fetch_weather(latitude, longitude, start_date, end_date):
    """Fetch weather data from Visual Crossing API for given coordinates and dates."""
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

    # Initialize list to store weather data
    weather_data = []

    # Fetch weather data for each row
    for row in df.iter_rows(named=True):
        latitude = row["latitude"]
        longitude = row["longitude"]
        date = row["observed_date"]

        # Fetch weather data
        weather_info = fetch_weather(latitude, longitude, date, date)

        if weather_info and "days" in weather_info:
            # Extract relevant weather details (e.g., temperature, humidity)
            day_data = weather_info["days"][0]
            row.update({
                "temp": day_data.get("temp", None),
                "humidity": day_data.get("humidity", None),
                "windspeed": day_data.get("windspeed", None),
                "dew": day_data.get("dew", None),
                "precip": day_data.get("precip", None),
                "snow": day_data.get("snow", None),
            })
        else:
            row.update({"temp": None, "humidity": None, "windspeed": None, "dew": None, "precip": None, "snow": None})

        weather_data.append(row)

    # Create a new DataFrame with added weather columns
    new_df = pl.DataFrame(weather_data)

    # Save updated CSV file to output directory
    output_file = output_folder / file.name
    new_df.write_csv(output_file)
    print(f"✅ Saved: {output_file}")

print("🎉 Processing complete!")
