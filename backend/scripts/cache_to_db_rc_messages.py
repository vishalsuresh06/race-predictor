from backend.database.models import RaceControlData
import pandas as pd

def get_rc_messages_data(year, event_name, round_number, session, session_data, db_session):
    rc_data = session_data.race_control_messages
    rc_rows = []

    existing_rc_data = db_session.query(RaceControlData).filter(
        RaceControlData.year == year,
        RaceControlData.event_name == event_name,
        RaceControlData.round_number == round_number,
        RaceControlData.session == session
    ).all()

    if not existing_rc_data:
        for _, row in rc_data.iterrows():
            rc_row = RaceControlData(
                year=year,
                event_name=event_name,
                round_number=round_number,
                session=session,
                time=row['Time'].timestamp() if pd.notna(row['Time']) else None,
                category=row['Category'] if pd.notna(row['Category']) else None,
                message=row['Message'] if pd.notna(row['Message']) else None,
                status=row['Status'] if pd.notna(row['Status']) else None,
                flag=row['Flag'] if pd.notna(row['Flag']) else None,
                scope=row['Scope'] if pd.notna(row['Scope']) else None,
                sector=row['Sector'] if pd.notna(row['Sector']) else None,
                racing_number=row['RacingNumber'] if pd.notna(row['RacingNumber']) else None,
                lap=row['Lap'] if pd.notna(row['Lap']) else None
            )
            rc_rows.append(rc_row)
        db_session.add_all(rc_rows)
        db_session.commit()
    else:
        print(f"Found existing race control messages for {event_name} {session}")