import os
from datetime import datetime, timedelta
from subprocess import call

def get_week_dates():
    current_date = datetime.now()
    start_week = current_date - timedelta(days=current_date.weekday())
    return [(start_week + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

def find_files(base_path):
    year_month = datetime.now().strftime('%Y-%m')
    directory = os.path.join(base_path, year_month)
    week_files = get_week_dates()
    return [os.path.join(directory, f"stocks_data_{day}.csv") for day in week_files if os.path.exists(os.path.join(directory, f"stocks_data_{day}.csv"))]

base_path = '../results/'
files = find_files(base_path)

# Call the MRJob script with the selected files
command = ['python', '1.py'] + files
call(command)
