import sys
import pandas as pd

print('arguments', sys.argv)

if len(sys.argv) < 2:
    print("Please provide a month!")
    sys.exit(1)

try:
    month = int(sys.argv[1])
except ValueError:
    print("Month must be an integer!")
    sys.exit(1)

# Create DataFrame
df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4]})
df['month'] = month
print(df.head())

# Save file
df.to_parquet(f"output_day_{month}.parquet")

print(f"hello pipeline, month={month}")
