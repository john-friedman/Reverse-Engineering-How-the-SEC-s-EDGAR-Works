import polars as pl
from datetime import time

# Read the CSV
df = pl.read_csv("data/detected_time.csv")

# Convert to datetime and convert to ET
df = df.with_columns([
    pl.col("detectedTime").str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ")
    .dt.replace_time_zone("UTC")
    .dt.convert_time_zone("US/Eastern"),
    pl.col("acceptanceDateTime").str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ")
    .dt.replace_time_zone("UTC")
    .dt.convert_time_zone("US/Eastern")
])

# Extract dates and times
df = df.with_columns([
    pl.col("detectedTime").dt.date().alias("detected_date"),
    pl.col("acceptanceDateTime").dt.date().alias("acceptance_date"),
    pl.col("acceptanceDateTime").dt.time().alias("acceptance_time")
])

# First acceptance time detected (earliest time of day after 1am)
after_1am_df = df.filter(pl.col("acceptance_time") > time(1, 0, 0))
first_acceptance = after_1am_df.filter(
    pl.col("acceptance_time") == after_1am_df.select(pl.col("acceptance_time").min()).item()
)

# Last acceptance time detected for same day
same_day_df = df.filter(pl.col("detected_date") == pl.col("acceptance_date"))
last_acceptance_same_day = same_day_df.filter(
    pl.col("acceptance_time") == same_day_df.select(pl.col("acceptance_time").max()).item()
)

# Last acceptance time detected for next day
next_day_df = df.filter(pl.col("detected_date") > pl.col("acceptance_date"))
last_acceptance_next_day = next_day_df.filter(
    pl.col("acceptance_time") == next_day_df.select(pl.col("acceptance_time").max()).item()
)

print("First acceptance time detected (after 1am):")
print(first_acceptance)
print("\nLast acceptance time detected for same day:")
print(last_acceptance_same_day)
print("\nLast acceptance time detected for next day:")
print(last_acceptance_next_day)