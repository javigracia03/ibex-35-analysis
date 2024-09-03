import os
import sys
from datetime import datetime, timedelta
from subprocess import call

def get_date_range_files(base_path, start_date, end_date):
    # Ensure date inputs are in the correct format (YYYY-MM-DD)
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    delta = timedelta(days=1)

    files = []
    while start_date <= end_date:
        year_month = start_date.strftime('%Y-%m')
        day = start_date.strftime('%Y-%m-%d')
        directory = os.path.join(base_path, year_month)
        file_path = os.path.join(directory, f"stocks_data_{day}.csv")
        if os.path.exists(file_path):
            files.append(file_path)
        start_date += delta
    return files

if __name__ == "__main__":
    # Check if enough arguments are provided
    if len(sys.argv) < 4:
        print("Usage: python script.py <start_date> <end_date> <action_name>")
        sys.exit(1)
    
    base_path = '../results/'  # Predefined base path
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    action_name = sys.argv[3]  # Capture the action name from command line

    files = get_date_range_files(base_path, start_date, end_date)

    # Call the MRJob script with the selected files and the action name
    command = ['python', '3.py', '-s' + action_name] + files
    print(f"Executing command: {' '.join(command)}")
    call(command)
