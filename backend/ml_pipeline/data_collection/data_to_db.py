import fastf1
import pandas as pd
from tqdm import tqdm
import time
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.scripts.cache_to_db_session_status import get_session_status_data
from backend.scripts.cache_to_db_weather import get_weather_data
from backend.scripts.cache_to_db_laps import get_laps_data
from backend.scripts.cache_to_db_telemetry import get_telemetry_data
from backend.scripts.cache_to_db_results import get_results_data
from backend.scripts.cache_to_db_track_status import get_track_status_data
from backend.scripts.cache_to_db_rc_messages import get_rc_messages_data

def main():
    engine = create_engine('sqlite:///backend/database/f1_data.db')
    Session = sessionmaker(bind=engine)
    db_session = Session()

    # Configure FastF1
    fastf1.Cache.enable_cache('backend/ml_pipeline/data_collection/f1_data_cache')
    fastf1.set_log_level('ERROR')

    YEAR = 2019  # Change this to the desired year
    SESSIONS = [
        'FP1',
        'FP2', 
        'FP3',
        'Q',
        'R'
    ]

    tqdm.write(f"[INFO] 🚀 Starting F1 data collection for {YEAR}")
    tqdm.write(f"[INFO] 📊 Sessions to collect: {', '.join(SESSIONS)}")

    # Get events for the year
    events = fastf1.get_event_schedule(YEAR)
    events = events[events['EventFormat'] == 'conventional']

    tqdm.write(f"[INFO] 🏁 Found {len(events)} events to process")
    tqdm.write("[INFO] " + "=" * 50)

    # Track statistics
    successful_sessions = 0
    failed_sessions = 0
    start_time = time.time()

    for _, event in tqdm(events.iterrows(), total=len(events), desc=f"Events in {YEAR}", position=0):

        # Create a single progress bar for sessions that gets updated
        with tqdm(SESSIONS, desc=f"Sessions in {event['EventName']}", position=1, leave=False) as session_pbar:
            for session in session_pbar:
                try:
                    # Update the progress bar description to show current session
                    session_pbar.set_description(f"Processing {event['EventName']} - {session}")
                        
                    #Load session data
                    session_data = fastf1.get_session(YEAR, str(event['RoundNumber']), session)
                    session_data.load(telemetry=True, weather=True, laps=True, messages=True)
                    
                    tqdm.write(f"Processing weather data for {event['EventName']} - {session}")
                    tqdm.write("=" * 50)
                    get_weather_data(YEAR, event['EventName'], event['RoundNumber'], session, session_data, db_session)
                    tqdm.write("=" * 50)

                    tqdm.write(f"Processing telemetry data for {event['EventName']} - {session}")
                    tqdm.write("=" * 50)
                    get_telemetry_data(YEAR, event['EventName'], event['RoundNumber'], session, session_data, db_session)
                    tqdm.write("=" * 50)

                    tqdm.write(f"Processing laps data for {event['EventName']} - {session}")
                    tqdm.write("=" * 50)
                    get_laps_data(YEAR, event['EventName'], event['RoundNumber'], session, session_data, db_session)
                    tqdm.write("=" * 50)

                    tqdm.write(f"Processing results data for {event['EventName']} - {session}")
                    tqdm.write("=" * 50)
                    get_results_data(YEAR, event['EventName'], event['RoundNumber'], session, session_data, db_session)
                    tqdm.write("=" * 50)

                    tqdm.write(f"Processing session status data for {event['EventName']} - {session}")
                    tqdm.write("=" * 50)
                    get_session_status_data(YEAR, event['EventName'], event['RoundNumber'], session, session_data, db_session)
                    tqdm.write("=" * 50)

                    tqdm.write(f"Processing track status data for {event['EventName']} - {session}")
                    tqdm.write("=" * 50)
                    get_track_status_data(YEAR, event['EventName'], event['RoundNumber'], session, session_data, db_session)
                    tqdm.write("=" * 50)

                    tqdm.write(f"Processing race control messages data for {event['EventName']} - {session}")
                    tqdm.write("=" * 50)
                    get_rc_messages_data(YEAR, event['EventName'], event['RoundNumber'], session, session_data, db_session)
                    tqdm.write("=" * 50)

                    # Update description to show completion
                    session_pbar.set_description(f"✓ {event['EventName']} - {session}")
                    successful_sessions += 1
                except Exception as e:
                    tqdm.write(f"[ERROR] Failed to process {event['EventName']} - {session}: {e}")
                    session_pbar.set_description(f"✗ {event['EventName']} - {session}")
                    failed_sessions += 1
                    cont = input("Do you want to continue with the next session? (y/n): ")
                    if cont.lower() != 'y':
                        continue
                    else:
                        sys.exit()

    # Print summary
    end_time = time.time()
    total_time = end_time - start_time

    tqdm.write("[INFO] " + "=" * 50)
    tqdm.write("[INFO] 📈 Data Collection Summary:")
    tqdm.write(f"[INFO] ✅ Successful sessions: {successful_sessions}")
    tqdm.write(f"[INFO] ❌ Failed sessions: {failed_sessions}")
    tqdm.write("[INFO] 🎉 Data collection completed!\n")
    tqdm.write(f"[INFO] ⏱ Total time taken: {total_time:.2f} seconds")
    tqdm.write("[INFO] " + "=" * 50)
            
    db_session.close()

if __name__ == "__main__":
    main()