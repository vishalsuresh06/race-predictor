import fastf1
import pandas as pd
from backend.database.models import LapData
from tqdm import tqdm

def get_laps_data(year, event_name, round_number, session, session_data, db_session):
    laps_data = session_data.laps
    laps_rows = []

    existing_laps_data = db_session.query(LapData).filter(
        LapData.year == year,
        LapData.event_name == event_name,
        LapData.round_number == round_number,
        LapData.session == session).all()
    
    if not existing_laps_data:
        for _, row in laps_data.iterrows():
            laps_row = LapData(
                year=year,
                event_name=event_name,
                round_number=round_number,
                session=session,
                time=row['Time'].total_seconds(),
                driver=row['Driver'],
                driver_number=row['DriverNumber'],
                lap_time=row['LapTime'].total_seconds(),
                lap_number=row['LapNumber'],
                stint=row['Stint'],
                pit_out_time=row['PitOutTime'].total_seconds(),
                pit_in_time=row['PitInTime'].total_seconds(),
                s1_time=row['S1Time'].total_seconds(),
                s2_time=row['S2Time'].total_seconds(),
                s3_time=row['S3Time'].total_seconds(),
                s1_session_time=row['S1SessionTime'].total_seconds(),
                s2_session_time=row['S2SessionTime'].total_seconds(),
                s3_session_time=row['S3SessionTime'].total_seconds(),
                s1_speedtrap=row['S1Speedtrap'],
                s2_speedtrap=row['S2Speedtrap'],
                fl_speedtrap=row['FLSpeedtrap'],
                st_speedtrap=row['STSpeedtrap'],
                is_personal_best=row['IsPersonalBest'],
                compound=row['Compound'],
                tyre_life=row['TyreLife'],
                fresh_tyre=row['FreshTyre'],
                team=row['Team'],
                lap_start_time=row['LapStartTime'].total_seconds(),
                lap_start_date=row['LapStartDate'],
                track_status=row['TrackStatus'],
                position=row['Position'],
                deleted=row['Deleted'],
                deleted_reason=row['DeletedReason'],
                fast_f1_generated=row['FastF1Generated'],
                is_accurate=row['IsAccurate']
            )
            laps_rows.append(laps_row)
        db_session.add_all(laps_rows)
        db_session.commit()
    else:
        tqdm.write(f"[INFO] Laps data already exists for {event_name} - {session}")