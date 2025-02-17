import os

# Define the folder containing the files
input_folder = "updated_weather_data_cities"

# Get a sorted list of CSV files in the folder
files = sorted([f for f in os.listdir(input_folder) if f.endswith(".csv")])

# Loop through the files and rename them with a number prefix
for index, file in enumerate(files, start=1):
    old_path = os.path.join(input_folder, file)
    new_name = f"{index}_{file}"
    new_path = os.path.join(input_folder, new_name)

    os.rename(old_path, new_path)
    print(f"Renamed: {file} → {new_name}")

print("Renaming complete!")
