import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load dataset
file_path = "../CompleteDataset/merged_output_cleaned.csv"
df = pd.read_csv(file_path)

# Calculate correlation coefficient
correlation_value = df["rain_sum"].corr(df["temperature_2m_mean"])
print(f"{correlation_value:.4f}")

# Create figure
plt.figure(figsize=(8, 5))

# Scatter plot with regression line
sns.regplot(x=df["rain_sum"], y=df["temperature_2m_mean"], data=df, scatter_kws={"alpha": 0.3}, line_kws={"color": "red"})



# Labels and title
plt.title("Temperature vs Rain (Regression Plot)")
plt.xlabel("Rain (mm)")
plt.ylabel("Temperature (°C)")

# Show plot
plt.show()


# -0.0724