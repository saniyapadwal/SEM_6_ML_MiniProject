import polars as pl
import os

# Read CSV
df = pl.read_csv("CompleteDataset/cleaned_wild_life_all_species.csv")
print("Original Shape:", df.shape)

# Convert observed_date to Date type
df = df.with_columns(
    pl.col("observed_date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
)

# Remove rows with observed_date < 2010
df = df.filter(
    pl.col("observed_date").is_not_null() & (pl.col("observed_date").dt.year() >= 2010)
)

print("Filtered Shape:", df.shape)

# Drop rows where 'city' is null
df = df.drop_nulls(subset=['city'])

# Ensure 'cities' directory exists
output_dir = "cities"
os.makedirs(output_dir, exist_ok=True)

# Save city-wise CSV files (convert city names to lowercase)
for city in df['city'].unique():
    city_lower = city.lower()  # Convert city name to lowercase
    city_df = df.filter(pl.col("city") == city)
    city_df.write_csv(f"{output_dir}/{city_lower}.csv")

print("City-wise CSV files saved successfully!")