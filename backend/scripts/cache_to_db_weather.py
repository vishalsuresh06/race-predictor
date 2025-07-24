import fastf1
from backend.database.models import WeatherData
from tqdm import tqdm

def get_weather_data(year, event_name, round_number, session, session_data, db_session):
    weather_data = session_data.weather_data
    weather_rows = []

    existing_weather_data = db_session.query(WeatherData).filter(
        WeatherData.year == year,
        WeatherData.event_name == event_name,
        WeatherData.round_number == round_number,
        WeatherData.session == session
    ).all()

    if not existing_weather_data:
        for _, row in weather_data.iterrows():
            weather_row = WeatherData(
                year=year,
                event_name=event_name,
                round_number=round_number,
                session=session,
                time=row['Time'].total_seconds(),
                air_temp=row['AirTemp'],
                humidity=row['Humidity'],
                pressure=row['Pressure'],
                rainfall=row['Rainfall'],
                track_temp=row['TrackTemp'],
                wind_direction=row['WindDirection'],
                wind_speed=row['WindSpeed']
            )
            weather_rows.append(weather_row)
        db_session.add_all(weather_rows)
        db_session.commit()
    else:
        tqdm.write(f"[INFO] Weather data already exists for {event_name} - {session}")