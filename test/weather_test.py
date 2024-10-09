import pytest
from io import StringIO
from interview.weather import process_csv, aggregate_daily_data
from datetime import datetime

@pytest.fixture
def sample_csv_data():
    return StringIO(
        """station,timestamp,temperature
Station1,06/01/2021 08:00:00 AM,70.5
Station1,06/01/2021 12:00:00 PM,75.1
Station1,06/01/2021 04:00:00 PM,80.0
Station1,06/02/2021 08:00:00 AM,72.3
Station2,06/01/2021 09:00:00 AM,65.2
Station2,06/01/2021 01:00:00 PM,68.9
Station2,06/02/2021 09:00:00 AM,66.5
"""
    )

def test_process_csv(sample_csv_data):
    result = process_csv(
        sample_csv_data,
        station_column="station",
        timestamp_column="timestamp",
        temp_column="temperature",
    )

    assert len(result) == 4
    assert result[0]["station"] == "Station1"
    assert result[0]["min_temp"] == 70.5
    assert result[0]["max_temp"] == 80.0

def test_aggregate_daily_data():
    station_data = {
        "Station1": {
            datetime(2021, 6, 1).date(): [
                (datetime(2021, 6, 1, 8, 0), 70.5),
                (datetime(2021, 6, 1, 12, 0), 75.1),
            ]
        }
    }
    result = aggregate_daily_data(station_data)

    assert len(result) == 1
    assert result[0]["first_temp"] == 70.5
    assert result[0]["last_temp"] == 75.1
    assert result[0]["max_temp"] == 75.1
    assert result[0]["min_temp"] == 70.5
