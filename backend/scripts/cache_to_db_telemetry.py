import pandas as pd
from backend.database.models import TelemetryData
from sqlalchemy import exists
from tqdm import tqdm

def get_telemetry_data(year, event_name, round_number, session, session_data, db_session):
    drivers = set(session_data.car_data.keys()) & set(session_data.pos_data.keys())  # only those with both

    for driver in drivers:
        # Quick existence check using exists() - much faster than loading all records
        data_exists = db_session.query(exists().where(
            TelemetryData.year == year,
            TelemetryData.event_name == event_name,
            TelemetryData.round_number == round_number,
            TelemetryData.session == session,
            TelemetryData.driver_number == driver
        )).scalar()

        if data_exists:
            tqdm.write(f"[INFO] Telemetry data already exists for {event_name} - {session} - {driver}")
            continue

        car_data = session_data.car_data[driver]
        pos_data = session_data.pos_data[driver]

        car_data = car_data.sort_values('SessionTime')
        pos_data = pos_data.sort_values('SessionTime')

        telemetry_data = pd.merge_asof(car_data, pos_data, on='SessionTime', direction='nearest')
        telemetry_data = telemetry_data.drop(columns=['Date_y', 'Time_y', 'Source_y'])
        telemetry_data = telemetry_data.rename(columns={'Date_x': 'Date', 'Time_x': 'Time', 'Source_x': 'Source'})
        desired_order = ['Date', 'Time', 'SessionTime','Source', 'Speed', 'RPM', 'nGear', 'Throttle', 'Brake', 'DRS', 'X', 'Y', 'Z', 'Status']
        telemetry_data = telemetry_data[desired_order]

        # Process in chunks to avoid memory issues and improve performance
        chunk_size = 2000  # Increased chunk size for better performance
        total_records = 0
        
        for i in range(0, len(telemetry_data), chunk_size):
            chunk = telemetry_data.iloc[i:i + chunk_size]
            
            # Use bulk_insert_mappings for much faster inserts
            telemetry_mappings = []
            for _, row in chunk.iterrows():
                telemetry_mapping = {
                    'year': year,
                    'event_name': event_name,
                    'round_number': round_number,
                    'session': session,
                    'driver_number': driver,
                    'time': row['Time'].total_seconds(),
                    'session_time': row['SessionTime'].total_seconds(),
                    'date': row['Date'],
                    'source': row['Source'],
                    'speed': row['Speed'],
                    'rpm': row['RPM'],
                    'n_gear': row['nGear'],
                    'throttle': row['Throttle'],
                    'brake': row['Brake'],
                    'drs': row['DRS'],
                    'x_pos': row['X'],
                    'y_pos': row['Y'],
                    'z_pos': row['Z'],
                    'status': row['Status']
                }
                telemetry_mappings.append(telemetry_mapping)
            
            # Bulk insert chunk
            if telemetry_mappings:
                db_session.bulk_insert_mappings(TelemetryData, telemetry_mappings)
                total_records += len(telemetry_mappings)
            
            # Clear memory
            del telemetry_mappings
        
        # Clear processed data from memory
        del telemetry_data, car_data, pos_data
        
        tqdm.write(f"[INFO] Processed {total_records} telemetry records for {event_name} - {session} - {driver}")
    
    # Note: No commit here - will be handled by the main transaction