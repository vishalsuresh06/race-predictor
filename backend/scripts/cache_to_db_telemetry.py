import pandas as pd
from backend.database.models import TelemetryData
from tqdm import tqdm

def get_telemetry_data(year, event_name, round_number, session, session_data, db_session):
    telemetry_data = session_data.telemetry_data
    telemetry_rows = []

    drivers = set(session_data.car_data.keys() + session_data.pos_data.keys())

    for driver in drivers:
        car_data = session_data.car_data[driver]
        pos_data = session_data.pos_data[driver]

        car_data = car_data.sort_values('SessionTime')
        pos_data = pos_data.sort_values('SessionTime')

        telemetry_data = pd.merge_asof(car_data, pos_data, on='SessionTime', direction='nearest')
        telemetry_data = telemetry_data.drop(columns=['Date_y', 'Time_y', 'Source_y'])
        telemetry_data = telemetry_data.rename(columns={'Date_x': 'Date', 'Time_x': 'Time', 'Source_x': 'Source'})
        desired_order = ['Date', 'Time', 'SessionTime','Source', 'Speed', 'RPM', 'nGear', 'Throttle', 'Brake', 'DRS', 'X', 'Y', 'Z', 'Status']
        telemetry_data = telemetry_data[desired_order]

        existing_telemetry_data = db_session.query(TelemetryData).filter(
            TelemetryData.year == year,
            TelemetryData.event_name == event_name,
            TelemetryData.round_number == round_number,
            TelemetryData.session == session,
            TelemetryData.driver_number == driver).all()

        if not existing_telemetry_data:
            for _, row in telemetry_data.iterrows():
                telemetry_row = TelemetryData(
                    year=year,
                    event_name=event_name,
                    round_number=round_number,
                    session=session,
                    driver_number=driver,
                    time=row['Time'].total_seconds(),
                    session_time=row['SessionTime'].total_seconds(),
                    date=row['Date'],
                    source=row['Source'],
                    speed=row['Speed'],
                    rpm=row['RPM'],
                    n_gear=row['nGear'],
                    throttle=row['Throttle'],
                    brake=row['Brake'],
                    drs=row['DRS'],
                    x_pos=row['X'],
                    y_pos=row['Y'],
                    z_pos=row['Z'],
                    status=row['Status']
                )
                telemetry_rows.append(telemetry_row)
            db_session.add_all(telemetry_rows)
            db_session.commit()
        else:
            tqdm.write(f"[INFO] Telemetry data already exists for {event_name} - {session} - {driver}")