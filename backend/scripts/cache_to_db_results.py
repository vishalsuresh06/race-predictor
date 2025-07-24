from backend.database.models import ResultsData
from tqdm import tqdm

def get_results_data(year, event_name, round_number, session, session_data, db_session):
    results_data = session_data.results
    results_rows = []

    existing_results_data = db_session.query(ResultsData).filter(
        ResultsData.year == year,
        ResultsData.event_name == event_name,
        ResultsData.round_number == round_number,
        ResultsData.session == session,).all()
    
    if not existing_results_data:
        for result in results_data:
            row = ResultsData(
                year=year,
                event_name=event_name,
                round_number=round_number,
                session=session,
                driver_number=results_data['DriverNumber'],
                broadcast_name=results_data['BroadcastName'],
                full_name=results_data['FullName'],
                abbreviation=results_data['Abbreviation'],
                team_name=results_data['TeamName'],
                team_color=results_data['TeamColor'],
                headshot_url=results_data['HeadshotUrl'],
                country_code=results_data['CountryCode'],
                position=results_data['Position'],
                classified_position=results_data['ClassifiedPosition'],
                grid_position=results_data['GridPosition'],
                q1=results_data['Q1'].total_seconds() if results_data['Q1'] else None,
                q2=results_data['Q2'].total_seconds() if results_data['Q2'] else None,
                q3=results_data['Q3'].total_seconds() if results_data['Q3'] else None,
                race_time=results_data['Time'].total_seconds() if results_data['Time'] else None,
                status=results_data['Status'],
                points=results_data['Points'],
                laps_completed=results_data['Laps']
            )
            results_rows.append(row)
        db_session.add_all(results_rows)
        db_session.commit()
    else:
        tqdm.write(f"[INFO] Results data already exists for {event_name} - {session}")