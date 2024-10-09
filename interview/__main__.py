import sys
from interview.weather import process_csv, output_to_stdout

aggregated_data = process_csv(
    sys.stdin,
    station_column="Station Name",
    timestamp_column="Measurement Timestamp",
    temp_column="Air Temperature",
)

output_to_stdout(aggregated_data)
