import fastf1
import pandas as pd
from tqdm import tqdm
import time

# Configure FastF1
fastf1.Cache.enable_cache('backend/data_collection/f1_data_cache')
fastf1.set_log_level('ERROR')

YEAR = 2024
SESSIONS = [
    'FP1',
    'FP2', 
    'FP3',
    'Q',
    'R'
]

print(f"🚀 Starting F1 data collection for {YEAR}")
print(f"📊 Sessions to collect: {', '.join(SESSIONS)}")

# Get events for the year
events = fastf1.get_event_schedule(YEAR)
events = events[events['EventFormat'] == 'conventional']

print(f"🏁 Found {len(events)} events to process")
print("=" * 50)

# Track statistics
successful_sessions = 0
failed_sessions = 0
start_time = time.time()

for event_index, event in tqdm(events.iterrows(), total=len(events), desc=f"Events in {YEAR}", position=0):
    
    # Create a single progress bar for sessions that gets updated
    with tqdm(SESSIONS, desc=f"Sessions in {event['EventName']}", position=1, leave=False) as session_pbar:
        for session in session_pbar:
            try:
                # Update the progress bar description to show current session
                session_pbar.set_description(f"Processing {event['EventName']} - {session}")
                
                session_data = fastf1.get_session(YEAR, str(event['RoundNumber']), session)
                session_data.load(telemetry=True, weather=True, laps=True, messages=True)
                
                # Update description to show completion
                session_pbar.set_description(f"✓ {event['EventName']} - {session}")
                successful_sessions += 1
            except Exception as e:
                session_pbar.set_description(f"✗ {event['EventName']} - {session}")
                failed_sessions += 1
                continue

# Print summary
end_time = time.time()
total_time = end_time - start_time

print("=" * 50)
print("📈 Data Collection Summary:")
print(f"✅ Successful sessions: {successful_sessions}")
print(f"❌ Failed sessions: {failed_sessions}")
print(f"⏱️  Total time: {total_time:.2f} seconds")
print(f"📊 Average time per session: {total_time/(successful_sessions + failed_sessions):.2f} seconds")
print("🎉 Data collection completed!")
        
