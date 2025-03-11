import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

file_path = "../CompleteDataset/merged_output_cleaned.csv"

df = pd.read_csv(file_path)


# Create a scatter plot of latitude vs longitude
plt.figure(figsize=(10, 6))

# Scatter plot
sns.scatterplot(x=df["longitude"], y=df["latitude"], alpha=0.5, color="blue")

# Labels and title
plt.title("Geolocation Scatter Plot (Latitude vs Longitude)")
plt.xlabel("Longitude")
plt.ylabel("Latitude")


# Show the plot
plt.show()


# Clearly shows the spatial distribution of data points.
# Highlights clusters of observations and potential outliers.
# Provides a direct geographical context for the dataset.


# Since the points are closely packed, longitude has a consistent effect on temperature.
# The temperature drop might be due to geographical factors like proximity to oceans, altitude, or climate zones.
