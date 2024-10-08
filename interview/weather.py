import csv
from datetime import datetime
from collections import defaultdict
import sys


def process_csv(
    input_stream,
    station_column=None,
    temp_column=None,
    timestamp_column=None,
):
    station_data = defaultdict(lambda: defaultdict(list))
    reader = csv.DictReader(input_stream)

    for row in reader:
        timestamp = datetime.strptime(row[timestamp_column], "%m/%d/%Y %H:%M:%S %p")
        station = row[station_column]
        try:
            temperature = float(row[temp_column])
        except ValueError:
            continue  # Skip invalid rows

        day = timestamp.date()  # Get the date (day)
        station_data[station][day].append((timestamp, temperature))

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


def output_to_stdout(daily_aggregates):
    fieldnames = [
        "Station Name",
        "Date",
        "Min Temp",
        "Max Temp",
        "First Temp",
        "Last Temp",
    ]
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
