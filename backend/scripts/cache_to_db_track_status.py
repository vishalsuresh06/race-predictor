from backend.database.models import TrackStatusData
from sqlalchemy import exists
import pandas as pd

def get_track_status_data(year, event_name, round_number, session, session_data, db_session):
    # Quick existence check using exists() - much faster than loading all records
    data_exists = db_session.query(exists().where(
        TrackStatusData.year == year,
        TrackStatusData.event_name == event_name,
        TrackStatusData.round_number == round_number,
        TrackStatusData.session == session
    )).scalar()
    
    if data_exists:
        print(f'Track Status data already exists for {event_name} {session}')
        return

    ts_data = session_data.track_status
    
    # Use bulk_insert_mappings for faster inserts
    ts_mappings = []
    for _, row in ts_data.iterrows():
        ts_mapping = {
            'year': year,
            'event_name': event_name,
            'round_number': round_number,
            'session': session,
            'time': row['Time'].total_seconds(),
            'status': row['Status'],
            'message': row['Message']
        }
        ts_mappings.append(ts_mapping)
    
    # Bulk insert - much faster than add_all
    if ts_mappings:
        db_session.bulk_insert_mappings(TrackStatusData, ts_mappings)
    
    print(f"Processed {len(ts_mappings)} track status records for {event_name} {session}")
    # Note: No commit here - will be handled by the main transaction