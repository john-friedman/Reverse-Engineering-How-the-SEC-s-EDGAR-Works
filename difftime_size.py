import polars as pl
import matplotlib.pyplot as plt
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

# Extract dates
df = df.with_columns([
    pl.col("detectedTime").dt.date().alias("detected_date"),
    pl.col("acceptanceDateTime").dt.date().alias("acceptance_date"),
    pl.col("acceptanceDateTime").dt.time().alias("acceptance_time")
])

# Filter for same-day detection
same_day = df.filter(pl.col("detected_date") == pl.col("acceptance_date"))

# Calculate time difference in seconds
same_day = same_day.with_columns([
    (pl.col("detectedTime") - pl.col("acceptanceDateTime")).dt.total_seconds().alias("difftime_seconds")
])

# Filter out zero or negative time differences for log scale
same_day_positive = same_day.filter(pl.col("difftime_seconds") > 0)

# Filter for 9:30 AM - 4:00 PM
market_hours = same_day_positive.filter(
    (pl.col("acceptance_time") >= time(9, 30, 0)) & 
    (pl.col("acceptance_time") <= time(16, 0, 0))
)

# Create figure with 2 subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))

# Plot 1: All filings
ax1.scatter(same_day_positive["size"],
           same_day_positive["difftime_seconds"],
           alpha=0.3, s=10)

ax1.set_xlabel("Filing Size (bytes) [Log Scale]")
ax1.set_ylabel("Time Difference (seconds) [Log Scale]")
ax1.set_xscale('log')
ax1.set_yscale('log')
ax1.set_title("All Same-Day Filings")
ax1.grid(True, alpha=0.3, which='both')

# Plot 2: Market hours (9:30 AM - 4:00 PM)
ax2.scatter(market_hours["size"],
           market_hours["difftime_seconds"],
           alpha=0.3, s=10)

ax2.set_xlabel("Filing Size (bytes) [Log Scale]")
ax2.set_ylabel("Time Difference (seconds) [Log Scale]")
ax2.set_xscale('log')
ax2.set_yscale('log')
ax2.set_title("Filings Accepted 9:30 AM - 4:00 PM")
ax2.grid(True, alpha=0.3, which='both')

plt.suptitle("SEC EDGAR Filings: Size vs Detection Delay", y=1.02, fontsize=14)
plt.tight_layout()
plt.savefig("plots/size_vs_difftime_all_and_market_hours_log_log.png", dpi=300, bbox_inches='tight')
plt.show()

# Print some summary statistics
print("All same-day filings:")
print(f"Total filings: {len(same_day_positive)}")
print(f"Mean size: {same_day_positive['size'].mean():.0f} bytes")
print(f"Median size: {same_day_positive['size'].median():.0f} bytes")
print(f"Mean difftime: {same_day_positive['difftime_seconds'].mean():.2f} seconds")
print(f"Median difftime: {same_day_positive['difftime_seconds'].median():.2f} seconds")

print("\nFilings during market hours (9:30 AM - 4:00 PM):")
print(f"Total filings: {len(market_hours)}")
print(f"Mean size: {market_hours['size'].mean():.0f} bytes")
print(f"Median size: {market_hours['size'].median():.0f} bytes")
print(f"Mean difftime: {market_hours['difftime_seconds'].mean():.2f} seconds")
print(f"Median difftime: {market_hours['difftime_seconds'].median():.2f} seconds")