#--------------------------------------------------------------#
#--------------------- Virtual Environment --------------------#
#--------------------------------------------------------------#

import sys
import pandas as pd

print("arguments:", sys.argv)


month = int(sys.argv[1])
year = int(sys.argv[2])

# Creation of a DataFrame
df = pd.DataFrame(
    {
        "day":[1,2],
        "passenger_number":[3, 4]
    }
)
df['month'] = month
df['year'] = year
# Display of the Dataframe head (first rows)
print(df.head())

# Save DataFrame to a parquet file
df.to_parquet(f"output_data_{sys.argv[1]}.parquet")

# Log processing information
print(f"Processing data for month = {month}, year = {year}")