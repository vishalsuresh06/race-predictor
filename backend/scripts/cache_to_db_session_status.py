import pandas as pd
from backend.database.models import SessionStatusData

def get_session_status_data(year, event_name, round_number, session, session_data, db_session):
    session_status_data = session_data.session_status
    session_status_rows = []

    existing_session_status_data = db_session.query(SessionStatusData).filter(
        SessionStatusData.year == year,
        SessionStatusData.event_name == event_name,
        SessionStatusData.round_number == round_number,
        SessionStatusData.session == session
    ).all()

    if not existing_session_status_data:
        for _, row in session_status_data.iterrows():
            session_status_row = SessionStatusData(
                year=year,
                event_name=event_name,
                round_number=round_number,
                session=session,
                time=row['Time'].total_seconds() if pd.notna(row['Time']) else None,
                status=row['Status'] if pd.notna(row['Status']) else None
            )
            session_status_rows.append(session_status_row)

        db_session.add_all(session_status_rows)
        db_session.commit()
    else:
        print(f"Session status data already exists for {event_name} {session}")
