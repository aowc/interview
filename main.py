# /usr/bin/env python3
"""
Usage: cat data/weather_chicago.csv | python main.py > output_data.csv
"""

import csv
from datetime import datetime
from collections import defaultdict
import sys


def process_csv(
    input_stream,
    station_column="Station Name",
    temp_column="Air Temperature",
    timestamp_column="Measurement Timestamp",
):
    # Dictionary to store daily data for each station
    station_data = defaultdict(lambda: defaultdict(list))

    # Read the CSV from input stream (stdin)
    reader = csv.DictReader(input_stream)

    for row in reader:
        timestamp = datetime.strptime(
            row[timestamp_column], "%m/%d/%Y %H:%M:%S %p"
        )
        station = row[station_column]
        try:
            temperature = float(row[temp_column])  # Convert temperature to float
        except ValueError:
            continue  # Skip invalid rows

        day = timestamp.date()  # Get the date (day)
        station_data[station][day].append((timestamp, temperature))

    # Calculate daily aggregates
    daily_aggregates = []
    for station, days in station_data.items():
        for day, entries in days.items():
            entries.sort()

            first_temp = entries[0][1]
            last_temp = entries[-1][1]
            max_temp = max(temp for _, temp in entries)
            min_temp = min(temp for _, temp in entries)

            daily_aggregates.append(
                {
                    "station": station,
                    "date": day,
                    "first_temp": first_temp,
                    "last_temp": last_temp,
                    "max_temp": max_temp,
                    "min_temp": min_temp,
                }
            )

    return daily_aggregates

def output_aggregates_to_stdout(daily_aggregates):
    fieldnames = ["Station Name", "Date", "Min Temp", "Max Temp", "First Temp", "Last Temp"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)

    writer.writeheader()
    for aggregate in daily_aggregates:
        writer.writerow(
            {
                "Station Name": aggregate["station"],
                "Date": aggregate["date"],
                "Min Temp": aggregate["min_temp"],
                "Max Temp": aggregate["max_temp"],
                "First Temp": aggregate["first_temp"],
                "Last Temp": aggregate["last_temp"],
            }
        )


def main():
    # Process the CSV input from stdin
    daily_aggregates = process_csv(sys.stdin)

    # Output the result to stdout
    output_aggregates_to_stdout(daily_aggregates)


if __name__ == "__main__":
    main()
