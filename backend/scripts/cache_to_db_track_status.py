from backend.database.models import TrackStatusData
import pandas as pd

def get_track_status_data(year, event_name, round_number, session, session_data, db_session):
    ts_data = session_data.track_status
    ts_rows = []

    existing_ts_data = db_session.query(TrackStatusData).filter(
        TrackStatusData.year == year,
        TrackStatusData.event_name == event_name,
        TrackStatusData.round_number == round_number,
        TrackStatusData.session == session
    ).all()
    if not existing_ts_data:
        for _, row in ts_data.iterrows():
            ts_row = TrackStatusData(
                year=year,
                event_name=event_name,
                round_number=round_number,
                session=session,
                time=row['Time'].total_seconds(),
                status=row['Status'],
                message=row['Message']
            )
            ts_rows.append(ts_row)
        db_session.add_all(ts_rows)
        db_session.commit()
    else:
        print(f'Track Status data already exists for {event_name} {session}')