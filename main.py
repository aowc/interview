# /usr/bin/env python3
"""
Usage: cat data/weather_chicago.csv | python main.py > output_data.csv
"""

import sys
import csv
from datetime import datetime
from collections import defaultdict

def process_csv(input_stream, output_stream):
    """
    Take input stream of csv data
    Output processed csv data
    """
    reader = csv.reader(input_stream)
    writer = csv.writer(output_stream)

    header = next(reader)
    station_index = header.index("Station Name")
    date_index = header.index("Measurement Timestamp")
    temp_index = header.index("Air Temperature")

    station_data = defaultdict(lambda: defaultdict(list))

    for row in reader:
        station = row[station_index]
        temperature = float(row[temp_index])
        date = row[date_index]

        # Still needs some more work here.
        # I should be putting some logic here to take all the entries and split them by day
        # making a list of the temperatures for the day
        station_data[station][date].append(temperature)

    writer.writerow(['Station Name', 'Date', 'Min Temp', 'Max Temp', 'First Temp', 'Last Temp'])

    for station, dates in station_data.items():
        for date, temperatures in dates.items():
            min_temp = min(temperatures)
            max_temp = max(temperatures)
            first_temp = temperatures[0]
            last_temp = temperatures[-1]

            writer.writerow([station, date, min_temp, max_temp, first_temp, last_temp])


def main():
    process_csv(sys.stdin, sys.stdout)


if __name__ == "__main__":
    main()
