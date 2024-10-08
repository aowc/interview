import sys
from interview import weather

weather.daily_aggregates = weather.process_csv(
    sys.stdin,
    station_column="Station Name",
    temp_column="Air Temperature",
    timestamp_column="Measurement Timestamp",
)
weather.output_to_stdout(weather.daily_aggregates)
