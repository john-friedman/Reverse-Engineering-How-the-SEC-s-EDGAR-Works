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

# Extract dates, times, and weekday
df = df.with_columns([
    pl.col("detectedTime").dt.date().alias("detected_date"),
    pl.col("acceptanceDateTime").dt.date().alias("acceptance_date"),
    pl.col("acceptanceDateTime").dt.time().alias("acceptance_time"),
    pl.col("detectedTime").dt.time().alias("detected_time_only"),
    pl.col("acceptanceDateTime").dt.weekday().alias("weekday")  # 5 = Friday
])

# Filter for same-day detection
same_day = df.filter(pl.col("detected_date") == pl.col("acceptance_date"))

# Filter for Friday 8-K filings
friday_8k = same_day.filter(
    (pl.col("weekday") == 5) & 
    (pl.col("submissionType") == "8-K")
)

# Convert times to seconds since midnight for plotting
same_day = same_day.with_columns([
    pl.col("acceptance_time").cast(pl.Duration).dt.total_seconds().alias("acceptance_seconds"),
    pl.col("detected_time_only").cast(pl.Duration).dt.total_seconds().alias("detected_seconds")
])

friday_8k = friday_8k.with_columns([
    pl.col("acceptance_time").cast(pl.Duration).dt.total_seconds().alias("acceptance_seconds"),
    pl.col("detected_time_only").cast(pl.Duration).dt.total_seconds().alias("detected_seconds")
])

# Create figure with 2 subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))

# Plot 1: Friday 8-K filings
ax1.scatter(friday_8k["acceptance_seconds"] / 3600,
           friday_8k["detected_seconds"] / 3600,
           alpha=0.5, s=20)

# Add vertical lines for market hours and SEC cutoff
market_open = 9.5  # 9:30 AM
market_close = 16.0  # 4:00 PM
sec_cutoff = 17.5  # 5:30 PM

ax1.axvline(x=market_open, color='green', linestyle='--', linewidth=1, alpha=0.5, label='Market Open (9:30 AM)')
ax1.axvline(x=market_close, color='red', linestyle='--', linewidth=1, alpha=0.5, label='Market Close (4:00 PM)')
ax1.axvline(x=sec_cutoff, color='orange', linestyle='--', linewidth=1, alpha=0.5, label='SEC Cutoff (5:30 PM)')

ax1.set_xlabel("Acceptance Time (hours)")
ax1.set_ylabel("Detection Time (hours)")
ax1.set_title("Friday 8-K Filings")
ax1.set_xlim(0, 24)
ax1.set_ylim(0, 24)
ax1.set_xticks(range(0, 25, 1))
ax1.set_yticks(range(0, 25, 1))
ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 1)], rotation=45, ha='right')
ax1.set_yticklabels([f"{h:02d}:00" for h in range(0, 25, 1)])
ax1.grid(True, alpha=0.3)
ax1.legend(loc='upper left')

# Plot 2: All same-day filings
ax2.scatter(same_day["acceptance_seconds"] / 3600,
           same_day["detected_seconds"] / 3600,
           alpha=0.3, s=10)

ax2.axvline(x=market_open, color='green', linestyle='--', linewidth=1, alpha=0.5)
ax2.axvline(x=market_close, color='red', linestyle='--', linewidth=1, alpha=0.5)
ax2.axvline(x=sec_cutoff, color='orange', linestyle='--', linewidth=1, alpha=0.5)

ax2.set_xlabel("Acceptance Time (hours)")
ax2.set_title("All Same-Day Filings (All Days)")
ax2.set_xlim(0, 24)
ax2.set_ylim(0, 24)
ax2.set_xticks(range(0, 25, 1))
ax2.set_yticks(range(0, 25, 1))
ax2.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 1)], rotation=45, ha='right')
ax2.set_yticklabels([])  # Remove y-axis tick labels
ax2.grid(True, alpha=0.3)

plt.suptitle("Acceptance Time vs Detection Time (Same Day)", y=1.02, fontsize=14)
plt.tight_layout()
plt.savefig("plots/friday_8k_vs_all_acceptance_vs_detection_time.png", dpi=300, bbox_inches='tight')
plt.show()

# Print summary statistics
print("Friday 8-K filings (same-day):")
print(f"Total filings: {len(friday_8k)}")
time_diff_friday = (friday_8k["detected_seconds"] - friday_8k["acceptance_seconds"])
print(f"Mean detection delay: {time_diff_friday.mean():.2f} seconds ({time_diff_friday.mean()/60:.2f} minutes)")
print(f"Median detection delay: {time_diff_friday.median():.2f} seconds ({time_diff_friday.median()/60:.2f} minutes)")

print("\nAll same-day filings:")
print(f"Total filings: {len(same_day)}")
time_diff_all = (same_day["detected_seconds"] - same_day["acceptance_seconds"])
print(f"Mean detection delay: {time_diff_all.mean():.2f} seconds ({time_diff_all.mean()/60:.2f} minutes)")
print(f"Median detection delay: {time_diff_all.median():.2f} seconds ({time_diff_all.median()/60:.2f} minutes)")