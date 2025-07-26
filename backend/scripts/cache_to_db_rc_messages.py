from backend.database.models import RaceControlData
from sqlalchemy import exists
import pandas as pd

def get_rc_messages_data(year, event_name, round_number, session, session_data, db_session):
    # Quick existence check using exists() - much faster than loading all records
    data_exists = db_session.query(exists().where(
        RaceControlData.year == year,
        RaceControlData.event_name == event_name,
        RaceControlData.round_number == round_number,
        RaceControlData.session == session
    )).scalar()

    if data_exists:
        print(f"Race control messages already exist for {event_name} {session}")
        return

    rc_data = session_data.race_control_messages
    
    # Use bulk_insert_mappings for faster inserts
    rc_mappings = []
    for _, row in rc_data.iterrows():
        rc_mapping = {
            'year': year,
            'event_name': event_name,
            'round_number': round_number,
            'session': session,
            'time': row['Time'].timestamp() if pd.notna(row['Time']) else None,
            'category': row['Category'] if pd.notna(row['Category']) else None,
            'message': row['Message'] if pd.notna(row['Message']) else None,
            'status': row['Status'] if pd.notna(row['Status']) else None,
            'flag': row['Flag'] if pd.notna(row['Flag']) else None,
            'scope': row['Scope'] if pd.notna(row['Scope']) else None,
            'sector': row['Sector'] if pd.notna(row['Sector']) else None,
            'racing_number': row['RacingNumber'] if pd.notna(row['RacingNumber']) else None,
            'lap': row['Lap'] if pd.notna(row['Lap']) else None
        }
        rc_mappings.append(rc_mapping)
    
    # Bulk insert - much faster than add_all
    if rc_mappings:
        db_session.bulk_insert_mappings(RaceControlData, rc_mappings)
    
    print(f"Processed {len(rc_mappings)} race control records for {event_name} {session}")
    # Note: No commit here - will be handled by the main transaction