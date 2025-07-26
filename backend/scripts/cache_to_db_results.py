from backend.database.models import ResultsData
from sqlalchemy import exists
import pandas as pd

def get_results_data(year, event_name, round_number, session, session_data, db_session):
    # Quick existence check using exists() - much faster than loading all records
    data_exists = db_session.query(exists().where(
        ResultsData.year == year,
        ResultsData.event_name == event_name,
        ResultsData.round_number == round_number,
        ResultsData.session == session
    )).scalar()
    
    if data_exists:
        print(f"Results data already exists for {event_name} {session}")
        return

    results_data = session_data.results
    
    # Use bulk_insert_mappings for faster inserts
    results_mappings = []
    for _, result in results_data.iterrows():
        results_mapping = {
            'year': year,
            'event_name': event_name,
            'round_number': round_number,
            'session': session,
            'driver_number': result['DriverNumber'] if pd.notna(result['DriverNumber']) else None,
            'broadcast_name': result['BroadcastName'] if pd.notna(result['BroadcastName']) else None,
            'full_name': result['FullName'] if pd.notna(result['FullName']) else None,
            'abbreviation': result['Abbreviation'] if pd.notna(result['Abbreviation']) else None,
            'team_name': result['TeamName'] if pd.notna(result['TeamName']) else None,
            'team_color': result['TeamColor'] if pd.notna(result['TeamColor']) else None,
            'headshot_url': result['HeadshotUrl'] if pd.notna(result['HeadshotUrl']) else None,
            'country_code': result['CountryCode'] if pd.notna(result['CountryCode']) else None,
            'position': result['Position'] if pd.notna(result['Position']) else None,
            'classified_position': result['ClassifiedPosition'] if pd.notna(result['ClassifiedPosition']) else None,
            'grid_position': result['GridPosition'] if pd.notna(result['GridPosition']) else None,
            'q1': result['Q1'].total_seconds() if pd.notna(result['Q1']) else None,
            'q2': result['Q2'].total_seconds() if pd.notna(result['Q2']) else None,
            'q3': result['Q3'].total_seconds() if pd.notna(result['Q3']) else None,
            'race_time': result['Time'].total_seconds() if pd.notna(result['Time']) else None,
            'status': result['Status'] if pd.notna(result['Status']) else None,
            'points': result['Points'] if pd.notna(result['Points']) else None,
            'laps_completed': result['Laps'] if pd.notna(result['Laps']) else None,
        }
        results_mappings.append(results_mapping)

    # Bulk insert - much faster than add_all
    if results_mappings:
        db_session.bulk_insert_mappings(ResultsData, results_mappings)
    
    print(f"Processed {len(results_mappings)} results records for {event_name} {session}")
    # Note: No commit here - will be handled by the main transaction