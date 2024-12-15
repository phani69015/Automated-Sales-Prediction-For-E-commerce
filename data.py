import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Set seed for reproducibility
np.random.seed(42)

# Generate a date range (let's assume daily data for one year)
date_range = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")

# Generate base sales data with a trend component
# Let's assume sales generally increase over the year, with some random fluctuations
trend = np.linspace(100, 500, len(date_range))

# Add seasonality (weekly pattern)
seasonality = 50 * np.sin(2 * np.pi * date_range.dayofweek / 7)

# Add some random noise
noise = np.random.normal(0, 20, len(date_range))

# Generate sales by combining trend, seasonality, and noise
sales = trend + seasonality + noise

# Ensure sales are non-negative
sales = np.maximum(sales, 0).astype(int)

# Create a DataFrame
df = pd.DataFrame({
    'Date': date_range,
    'Sales': sales
})

# Feature Engineering
df['DayOfWeek'] = df['Date'].dt.dayofweek
df['Month'] = df['Date'].dt.month
df['IsWeekend'] = df['DayOfWeek'].apply(lambda x: 1 if x >= 5 else 0)

# Add lag features (e.g., last week's sales as a feature)
df['Sales_Lag_7'] = df['Sales'].shift(7)

# Drop missing values due to lag
df = df.dropna()

# Display the first few rows
print(df.head())

# Plot the generated sales data
plt.figure(figsize=(10, 6))
plt.plot(df['Date'], df['Sales'], label='Sales')
plt.title('Synthetic Sales Data')
plt.xlabel('Date')
plt.ylabel('Sales')
plt.legend()
plt.show()

# Save the synthetic data to a CSV file
df.to_csv('data.csv', index=False)
