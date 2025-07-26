import fastf1
from backend.database.models import WeatherData
from sqlalchemy import exists
from tqdm import tqdm

def get_weather_data(year, event_name, round_number, session, session_data, db_session):
    # Quick existence check using exists() - much faster than loading all records
    data_exists = db_session.query(exists().where(
        WeatherData.year == year,
        WeatherData.event_name == event_name,
        WeatherData.round_number == round_number,
        WeatherData.session == session
    )).scalar()

    if data_exists:
        tqdm.write(f"[INFO] Weather data already exists for {event_name} - {session}")
        return

    weather_data = session_data.weather_data
    
    # Use bulk_insert_mappings for faster inserts
    weather_mappings = []
    for _, row in weather_data.iterrows():
        weather_mapping = {
            'year': year,
            'event_name': event_name,
            'round_number': round_number,
            'session': session,
            'time': row['Time'].total_seconds(),
            'air_temp': row['AirTemp'],
            'humidity': row['Humidity'],
            'pressure': row['Pressure'],
            'rainfall': row['Rainfall'],
            'track_temp': row['TrackTemp'],
            'wind_direction': row['WindDirection'],
            'wind_speed': row['WindSpeed']
        }
        weather_mappings.append(weather_mapping)
    
    # Bulk insert - much faster than add_all
    if weather_mappings:
        db_session.bulk_insert_mappings(WeatherData, weather_mappings)
    
    tqdm.write(f"[INFO] Processed {len(weather_mappings)} weather records for {event_name} - {session}")
    # Note: No commit here - will be handled by the main transaction