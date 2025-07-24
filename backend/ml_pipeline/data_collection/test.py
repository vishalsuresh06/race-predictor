import fastf1
import pandas as pd

session = fastf1.get_session(2024, '1', 'FP1')
session.load(telemetry=True, weather=True, laps=True, messages=True)

# car_data = session.car_data['63']
# pos_data = session.pos_data['63']
# weather_data = session.weather_data

# print(weather_data.head())
# print(car_data.head())
# print(pos_data.head())

# # Ensure SessionTime is sorted (required for merge_asof)
# car_data = car_data.sort_values('SessionTime')
# pos_data = pos_data.sort_values('SessionTime')

# # Align the two DataFrames using a nearest merge on SessionTime
# merged_data = pd.merge_asof(car_data, pos_data, on='SessionTime', direction='nearest')
# merged_data = merged_data.drop(columns=['Date_y', 'Time_y', 'Source_y'])
# merged_data = merged_data.rename(columns={'Date_x': 'Date', 'Time_x': 'Time', 'Source_x': 'Source'})
# desired_order = ['Date', 'Time', 'SessionTime','Source', 'Speed', 'RPM', 'nGear', 'Throttle', 'Brake', 'DRS', 'X', 'Y', 'Z', 'Status']
# merged_data = merged_data[desired_order]

# merged_data.to_csv('backend/ml_pipeline/data_collection/merged_data.csv', index=False)

list1 = [1,2]
list2 = [3,4]

print(list1.extend(list2))
