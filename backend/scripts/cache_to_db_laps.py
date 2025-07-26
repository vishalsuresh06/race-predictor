from math import isnan
import pandas as pd
from backend.database.models import LapData
from sqlalchemy import exists

def get_laps_data(year, event_name, round_number, session, session_data, db_session):
    # Quick existence check using exists() - much faster than loading all records
    data_exists = db_session.query(exists().where(
        LapData.year == year,
        LapData.event_name == event_name,
        LapData.round_number == round_number,
        LapData.session == session
    )).scalar()
    
    if data_exists:
        print(f"Laps data already exists for {event_name} {session}")
        return

    laps_data = session_data.laps
    
    # Use bulk_insert_mappings for much faster inserts
    lap_mappings = []
    for _, lap in laps_data.iterrows():
        lap_mapping = {
            'year': year,
            'event_name': event_name,
            'round_number': round_number,
            'session': session,
            'driver': lap['Driver'],
            'driver_number': lap['DriverNumber'],
            'lap_time': lap['LapTime'].total_seconds() if pd.notna(lap['LapTime']) else None,
            'lap_number': lap['LapNumber'],
            'stint': lap['Stint'] if pd.notna(lap['Stint']) else None,
            'pit_out_time': lap['PitOutTime'].total_seconds() if pd.notna(lap['PitOutTime']) else None,
            'pit_in_time': lap['PitInTime'].total_seconds() if pd.notna(lap['PitInTime']) else None,
            's1_time': lap['Sector1Time'].total_seconds() if pd.notna(lap['Sector1Time']) else None,
            's2_time': lap['Sector2Time'].total_seconds() if pd.notna(lap['Sector2Time']) else None,
            's3_time': lap['Sector3Time'].total_seconds() if pd.notna(lap['Sector3Time']) else None,
            's1_session_time': lap['Sector1SessionTime'].total_seconds() if pd.notna(lap['Sector1SessionTime']) else None,
            's2_session_time': lap['Sector2SessionTime'].total_seconds() if pd.notna(lap['Sector2SessionTime']) else None,
            's3_session_time': lap['Sector3SessionTime'].total_seconds() if pd.notna(lap['Sector3SessionTime']) else None,
            's1_speedtrap': lap['SpeedI1'] if pd.notna(lap['SpeedI1']) else None,
            's2_speedtrap': lap['SpeedI2'] if pd.notna(lap['SpeedI2']) else None,
            'fl_speedtrap': lap['SpeedFL'] if pd.notna(lap['SpeedFL']) else None,
            'st_speedtrap': lap['SpeedST'] if pd.notna(lap['SpeedST']) else None,
            'is_personal_best': bool(lap['IsPersonalBest']) if pd.notna(lap['IsPersonalBest']) else None,
            'compound': lap['Compound'] if pd.notna(lap['Compound']) else None,
            'tyre_life': int(lap['TyreLife']) if pd.notna(lap['TyreLife']) else None,
            'fresh_tyre': bool(lap['FreshTyre']) if pd.notna(lap['FreshTyre']) else None,
            'team': lap['Team'] if pd.notna(lap['Team']) else None,
            'lap_start_time': lap['LapStartTime'].total_seconds() if pd.notna(lap['LapStartTime']) else None,
            'lap_start_date': lap['LapStartDate'] if pd.notna(lap['LapStartDate']) else None,
            'track_status': lap['TrackStatus'] if pd.notna(lap['TrackStatus']) else None,
            'position': lap['Position'] if pd.notna(lap['Position']) else None,
            'deleted': bool(lap['Deleted']) if pd.notna(lap['Deleted']) else None,
            'deleted_reason': lap['DeletedReason'] if pd.notna(lap['DeletedReason']) else None,
            'fast_f1_generated': bool(lap['FastF1Generated']) if pd.notna(lap['FastF1Generated']) else None,
            'is_accurate': bool(lap['IsAccurate']) if pd.notna(lap['IsAccurate']) else None,
        }
        lap_mappings.append(lap_mapping)

    # Bulk insert - much faster than add_all
    if lap_mappings:
        db_session.bulk_insert_mappings(LapData, lap_mappings)
    
    print(f"Processed {len(lap_mappings)} lap records for {event_name} {session}")
    # Note: No commit here - will be handled by the main transaction