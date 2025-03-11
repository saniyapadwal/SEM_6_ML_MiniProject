import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load dataset
file_path = "../CompleteDataset/merged_output_cleaned.csv"
df = pd.read_csv(file_path)

# Create subplots for both regression plots
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

correlation_value1 = df["temperature_2m_mean"].corr(df["latitude"])
correlation_value2 = df["temperature_2m_mean"].corr(df["longitude"])

print(f"{correlation_value1, correlation_value2}")

# Scatter Plot 1: Latitude vs Temperature
sns.regplot(x=df["temperature_2m_mean"], y=df["latitude"], data=df, scatter_kws={"alpha": 0.3}, line_kws={"color": "red"}, ax=axes[0])
axes[0].set_xlabel("Temperature (°C)")
axes[0].set_ylabel("Latitude")
axes[0].set_title("Latitude vs Temperature (Regression Plot)")

# Scatter Plot 2: Longitude vs Temperature
sns.regplot(x=df["temperature_2m_mean"], y=df["longitude"], data=df, scatter_kws={"alpha": 0.3}, line_kws={"color": "red"}, ax=axes[1])
axes[1].set_xlabel("Temperature (°C)")
axes[1].set_ylabel("Longitude")
axes[1].set_title("Longitude vs Temperature (Regression Plot)")

# Show plot
plt.tight_layout()
plt.show()



# If latitude decreases as temperature increases, the regression line will slope downward (negative correlation).
# This would mean hotter temperatures at lower latitudes (closer to the equator) and cooler temperatures at higher latitudes.

# -0.2066917756632544
# -0.10999441065106876

# The Prime Meridian is the zero-degree longitude (0°), dividing the Earth into Eastern and Western Hemispheres.