from interview import weather
import io


def test_process_csv():
    input_data = """station,timestamp,air_temperature
    Station1,12/31/2016 11:00:00 PM,65.0
    Station1,12/31/2016 12:00:00 PM,70.0
    Station1,12/30/2016 11:00:00 PM,68.0
    Station1,12/30/2016 12:00:00 PM,60.0
    """
    input_stream = io.StringIO(input_data)
    output_stream = io.StringIO()

    weather.process_csv(
        input_stream,
        station_column="station",
        timestamp_column="timestamp",
        temp_column="air_temperature",
    )

    output_stream.seek(0)
    reader = csv.DictReader(output_stream)

    results = list(reader)
    assert len(results) == 2  # Two days
    assert results[0]["station"] == "Station1"
    assert results[0]["date"] == "12/31/2016"
    assert results[0]["min_temp"] == "65.0"
    assert results[0]["max_temp"] == "68.0"
    assert results[0]["first_temp"] == "65.0"
    assert results[0]["last_temp"] == "68.0"
    assert results[1]["date"] == "12/30/2016"
    assert results[1]["min_temp"] == "60.0"
    assert results[1]["max_temp"] == "68.0"
    assert results[1]["first_temp"] == "68.0"
    assert results[1]["last_temp"] == "60.0"
