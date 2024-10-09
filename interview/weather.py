import sys
import csv
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any, IO, Tuple


def process_csv(
    input_stream: IO[str],
    station_column: str,
    timestamp_column: str,
    temp_column: str,
) -> List[Dict[str, Any]]:
    station_data = defaultdict(lambda: defaultdict(list))
    reader = csv.DictReader(input_stream)

    for row in reader:
        timestamp_str = row.get(timestamp_column)
        station = row.get(station_column)
        temp_str = row.get(temp_column)

        if not timestamp_str or not station or not temp_str:
            continue

        try:
            timestamp = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S %p")
            temperature = float(temp_str)
        except (ValueError, KeyError):
            continue

        day = timestamp.date().strftime("%m/%d/%Y")
        station_data[station][day].append((timestamp, temperature))

    return aggregate_daily_data(station_data)


def aggregate_daily_data(
    station_data: Dict[str, Dict[datetime, List[Tuple[datetime, float]]]]
) -> List[Dict[str, Any]]:
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


def output_to_stdout(daily_aggregates: List[Dict[str, Any]]) -> None:
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
