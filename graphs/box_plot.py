import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load dataset
file_path = "../CompleteDataset/merged_output_cleaned.csv"
df = pd.read_csv(file_path)

# Step 1: Calculate Q1, Q3, and IQR for latitude
Q1 = df["latitude"].quantile(0.25)
Q3 = df["latitude"].quantile(0.75)
IQR = Q3 - Q1

# Step 2: Define lower and upper bounds
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Step 3: Remove outliers (keep only values within bounds)
df_cleaned = df[(df["latitude"] >= lower_bound) & (df["latitude"] <= upper_bound)]

# Step 4: Plot Box Plot (Before & After Outlier Removal)
plt.figure(figsize=(12, 5))

# Original Data
plt.subplot(1, 2, 1)
sns.boxplot(x=df["latitude"], color='red')
plt.title("Before Removing Outliers")

# Cleaned Data
plt.subplot(1, 2, 2)
sns.boxplot(x=df_cleaned["latitude"], color='green')
plt.title("After Removing Outliers")

plt.show()

# Print the number of removed outliers
print(f"Original data size: {len(df)}")
print(f"Cleaned data size: {len(df_cleaned)}")
print(f"Number of outliers removed: {len(df) - len(df_cleaned)}")