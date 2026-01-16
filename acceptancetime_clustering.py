import polars as pl
import matplotlib.pyplot as plt

# Read the original CSV
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
    pl.col("acceptanceDateTime").dt.date().alias("acceptance_date")
])

# Round acceptanceDateTime to 5-minute intervals and extract time only
df = df.with_columns([
    (pl.col("acceptanceDateTime").dt.truncate("5m")).alias("interval_5m"),
    (pl.col("acceptanceDateTime").dt.truncate("5m").dt.time()).alias("time_only")
])

# Count filings per date-time interval (to calculate daily totals)
daily_time_counts = df.group_by(["acceptance_date", "time_only"]).agg(
    pl.count().alias("filing_count")
)

# Calculate total filings per day
daily_totals = df.group_by("acceptance_date").agg(
    pl.count().alias("daily_total")
)

# Join to get daily totals for each interval
daily_time_counts = daily_time_counts.join(daily_totals, on="acceptance_date")

# Calculate percentage for each interval
daily_time_counts = daily_time_counts.with_columns(
    (pl.col("filing_count") / pl.col("daily_total") * 100).alias("percent_of_day")
)

# Average the percentages across all dates for each time
interval_percentages = daily_time_counts.group_by("time_only").agg(
    pl.col("percent_of_day").mean().alias("avg_percent_of_day")
).sort("time_only")

# Convert time to seconds since midnight for plotting
interval_percentages = interval_percentages.with_columns(
    (pl.col("time_only").cast(pl.Utf8).str.strptime(pl.Time, "%H:%M:%S")
     .cast(pl.Duration).dt.total_seconds()).alias("seconds_since_midnight")
)

# Calculate cumulative sum
interval_percentages = interval_percentages.with_columns(
    pl.col("avg_percent_of_day").cum_sum().alias("cumulative_percent")
)

# Create first figure with subplots (linear scale)
fig1, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

# Plot 1: Regular percentage
ax1.scatter(interval_percentages["seconds_since_midnight"] / 3600,
           interval_percentages["avg_percent_of_day"], 
           s=20, alpha=0.6)

# Add vertical lines for market hours and SEC cutoff
market_open = 9.5  # 9:30 AM
market_close = 16.0  # 4:00 PM
sec_cutoff = 17.5  # 5:30 PM

ax1.axvline(x=market_open, color='green', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Open (9:30 AM)')
ax1.axvline(x=market_close, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Close (4:00 PM)')
ax1.axvline(x=sec_cutoff, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='SEC EDGAR Cutoff (5:30 PM)')

ax1.set_ylabel("Percent of Daily Filings Accepted (%)")
ax1.set_title("SEC EDGAR Filings")
ax1.grid(True, alpha=0.3)
ax1.set_xlim(0, 24)
ax1.set_xticks(range(0, 25, 1))
ax1.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 1)], rotation=0, ha='center')
ax1.legend(loc='upper right')

# Plot 2: Cumulative sum
ax2.plot(interval_percentages["seconds_since_midnight"] / 3600,
         interval_percentages["cumulative_percent"],
         linewidth=2, alpha=0.8)

ax2.axvline(x=market_open, color='green', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Open (9:30 AM)')
ax2.axvline(x=market_close, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Close (4:00 PM)')
ax2.axvline(x=sec_cutoff, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='SEC EDGAR Cutoff (5:30 PM)')

# Add horizontal line at 50%
ax2.axhline(y=50, color='purple', linestyle=':', linewidth=1.5, alpha=0.7, label='50% of Daily Filings')

ax2.set_ylabel("Cumulative Percent of Daily Filings Accepted (%)")
ax2.grid(True, alpha=0.3)
ax2.set_xlim(0, 24)
ax2.set_ylim(0, 100)
ax2.set_xticks(range(0, 25, 1))
ax2.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 1)], rotation=0, ha='center')
ax2.legend(loc='lower right')

plt.tight_layout()
plt.savefig("plots/filings_acceptance_percent_and_cumsum_per_5m_interval_by_time.png", dpi=300)
plt.show()

# Create second figure with log scale
fig2, (ax3, ax4) = plt.subplots(2, 1, figsize=(14, 10))

# Plot 3: Regular percentage with log scale
ax3.scatter(interval_percentages["seconds_since_midnight"] / 3600,
           interval_percentages["avg_percent_of_day"], 
           s=20, alpha=0.6)

ax3.axvline(x=market_open, color='green', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Open (9:30 AM)')
ax3.axvline(x=market_close, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Close (4:00 PM)')
ax3.axvline(x=sec_cutoff, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='SEC EDGAR Cutoff (5:30 PM)')

ax3.set_ylabel("Percent of Daily Filings Accepted (%) [Log Scale]")
ax3.set_title("SEC EDGAR Filings")
ax3.set_yscale('log')
ax3.grid(True, alpha=0.3, which='both')
ax3.set_xlim(0, 24)
ax3.set_xticks(range(0, 25, 1))
ax3.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 1)], rotation=0, ha='center')
ax3.legend(loc='upper right')

# Plot 4: Cumulative sum (same as before, no log scale makes sense here)
ax4.plot(interval_percentages["seconds_since_midnight"] / 3600,
         interval_percentages["cumulative_percent"],
         linewidth=2, alpha=0.8)

ax4.axvline(x=market_open, color='green', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Open (9:30 AM)')
ax4.axvline(x=market_close, color='red', linestyle='--', linewidth=1.5, alpha=0.7, label='Market Close (4:00 PM)')
ax4.axvline(x=sec_cutoff, color='orange', linestyle='--', linewidth=1.5, alpha=0.7, label='SEC EDGAR Cutoff (5:30 PM)')

ax4.axhline(y=50, color='purple', linestyle=':', linewidth=1.5, alpha=0.7, label='50% of Daily Filings')

ax4.set_ylabel("Cumulative Percent of Daily Filings Accepted (%)")
ax4.grid(True, alpha=0.3)
ax4.set_xlim(0, 24)
ax4.set_ylim(0, 100)
ax4.set_xticks(range(0, 25, 1))
ax4.set_xticklabels([f"{h:02d}:00" for h in range(0, 25, 1)], rotation=0, ha='center')
ax4.legend(loc='lower right')

plt.tight_layout()
plt.savefig("plots/filings_acceptance_percent_and_cumsum_per_5m_interval_by_time_log.png", dpi=300)
plt.show()