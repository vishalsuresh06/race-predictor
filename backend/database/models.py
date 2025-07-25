from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Interval
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.sql import func

engine = create_engine('sqlite:///backend/database/f1_data.db', future=True)

Base = declarative_base()

class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    event_name = Column(String)
    round_number = Column(Integer)
    session = Column(String)
    time = Column(Float)  # seconds
    air_temp = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    rainfall = Column(Boolean)
    track_temp = Column(Float)
    wind_direction = Column(Integer)
    wind_speed = Column(Float)

class TelemetryData(Base):
    __tablename__ = 'telemetry_data'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    event_name = Column(String)
    round_number = Column(Integer)
    session = Column(String)
    driver_number = Column(String)
    time = Column(Float)  # seconds
    session_time = Column(Float)  # seconds
    date = Column(DateTime)  # naive
    source = Column(String)
    speed = Column(Float)
    rpm = Column(Float)
    n_gear = Column(Integer)
    throttle = Column(Float)
    brake = Column(Boolean)
    drs = Column(Integer)
    x_pos = Column(Float)
    y_pos = Column(Float)
    z_pos = Column(Float)
    status = Column(String)

class LapData(Base):
    __tablename__ = 'lap_data'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    event_name = Column(String)
    round_number = Column(Integer)
    session = Column(String)
    time = Column(Float)  # seconds
    driver = Column(String)
    driver_number = Column(Integer)
    lap_time = Column(Float)  # seconds
    lap_number = Column(Float)
    stint = Column(Float)
    pit_out_time = Column(Float)  # seconds
    pit_in_time = Column(Float)  # seconds
    s1_time = Column(Float)  # seconds
    s2_time = Column(Float)  # seconds
    s3_time = Column(Float)  # seconds
    s1_session_time = Column(Float)  # seconds
    s2_session_time = Column(Float)  # seconds
    s3_session_time = Column(Float)  # seconds
    s1_speedtrap = Column(Float)
    s2_speedtrap = Column(Float)
    fl_speedtrap = Column(Float)
    st_speedtrap = Column(Float)
    is_personal_best = Column(Boolean)
    compound = Column(String)
    tyre_life = Column(Float)
    fresh_tyre = Column(Boolean)
    team = Column(String)
    lap_start_time = Column(Float)  # seconds
    lap_start_date = Column(DateTime)  # naive
    track_status = Column(String)
    position = Column(Float)
    deleted = Column(Boolean)
    deleted_reason = Column(String)
    fast_f1_generated = Column(Boolean)
    is_accurate = Column(Boolean)

class ResultsData(Base):
    __tablename__ = 'results_data'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    event_name = Column(String)
    round_number = Column(Integer)
    session = Column(String)
    driver_number = Column(String)
    broadcast_name = Column(String)
    full_name = Column(String)
    abbreviation = Column(String)
    team_name = Column(String)
    team_color = Column(String)
    headshot_url = Column(String)
    country_code = Column(String)
    position = Column(Float)
    classified_position = Column(String)
    grid_position = Column(Float)
    q1 = Column(Float)
    q2 = Column(Float)
    q3 = Column(Float)
    race_time = Column(Float)
    status = Column(String)
    points = Column(Float)
    laps_completed = Column(Float)

class SessionStatusData(Base):
    __tablename__ = 'session_status_data'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    event_name = Column(String)
    round_number = Column(Integer)
    session = Column(String)
    time = Column(Float)
    status = Column(String)

class TrackStatusData(Base):
    __tablename__ = 'track_status_data'

    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    event_name = Column(String)
    round_number = Column(Integer)
    session = Column(String)
    time = Column(Float)
    status = Column(String)
    message = Column(String)

class RaceControlData(Base):
    __tablename__ = 'race_control_data'
    
    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    event_name = Column(String)
    round_number = Column(Integer)
    session = Column(String)
    time = Column(Float)
    category = Column(String)
    message = Column(String)
    # New columns added for additional
    status = Column(String)
    flag = Column(String)
    scope = Column(String)
    sector = Column(Integer)
    racing_number = Column(String)
    lap = Column(Integer)


Base.metadata.create_all(engine)