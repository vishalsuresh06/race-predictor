import pandas as pd
from backend.database.models import SessionStatusData
from sqlalchemy import exists

def get_session_status_data(year, event_name, round_number, session, session_data, db_session):
    # Quick existence check using exists() - much faster than loading all records
    data_exists = db_session.query(exists().where(
        SessionStatusData.year == year,
        SessionStatusData.event_name == event_name,
        SessionStatusData.round_number == round_number,
        SessionStatusData.session == session
    )).scalar()

    if data_exists:
        print(f"Session status data already exists for {event_name} {session}")
        return

    session_status_data = session_data.session_status
    
    # Use bulk_insert_mappings for faster inserts
    session_status_mappings = []
    for _, row in session_status_data.iterrows():
        session_status_mapping = {
            'year': year,
            'event_name': event_name,
            'round_number': round_number,
            'session': session,
            'time': row['Time'].total_seconds() if pd.notna(row['Time']) else None,
            'status': row['Status'] if pd.notna(row['Status']) else None
        }
        session_status_mappings.append(session_status_mapping)

    # Bulk insert - much faster than add_all
    if session_status_mappings:
        db_session.bulk_insert_mappings(SessionStatusData, session_status_mappings)
    
    print(f"Processed {len(session_status_mappings)} session status records for {event_name} {session}")
    # Note: No commit here - will be handled by the main transaction