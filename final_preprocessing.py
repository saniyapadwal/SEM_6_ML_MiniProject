import pandas as pd


# Define the file path
file_path = "CompleteDataset/merged_output.csv"


# Load the Dataset
df = pd.read_csv(file_path)


# Display the first and last few rows to understand the structure of the dataset
print(df.head()) # View first 5 rows
print(df.tail()) # View Last 5 rows


# Display information about the dataset (columns, data types, non-null counts)
print(df.info())


# Display summary statistics for numerical columns
print(df.describe())


# Check for missing values in each colmun
print(df.isnull().sum())


# Check for duplicate rows in the dataset
print(df.duplicated().sum())


# Handle missing values using group-wise imputation (median per city and state)
weather_columns = [
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max"
]

# Fill missing values with the median of each city-state group
df[weather_columns] = df.groupby(["city", "state"])[weather_columns].transform(lambda x: x.fillna(x.median()))


# If there are still missing values (i.e., for cities with all missing data), fill with overall median
df[weather_columns] = df[weather_columns].fillna(df[weather_columns].median())


# Verify if missing values are handled
print(df.isnull().sum())


# Save the cleaned dataset
df.to_csv("CompleteDataset/merged_output_cleaned.csv", index=False)