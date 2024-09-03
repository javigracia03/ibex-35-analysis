import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from subprocess import call

def get_last_month_dates():
    today = datetime.today()
    last_month = today - timedelta(days=30)  # Assuming each month has 30 days
    start_date = last_month.replace(day=1)
    return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')



def get_date_range_files(base_path, start_date, end_date):
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

def concatenate_csv(files):
    dfs = []
    for file in files:
        file_name = os.path.basename(file)
        date_str = file_name.split('_')[-1].split('.')[0]
        date = datetime.strptime(date_str, '%Y-%m-%d')
        with open(file, 'r', encoding='latin-1') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) == 5: 
                    stock_name, open_price, high_price, low_price, hour= parts[:5]
                    df = pd.DataFrame([[stock_name, float(open_price), float(high_price), float(low_price), hour ,date]], columns=['Stock', 'Open', 'High', 'Low', 'Hour', 'Date'])
                    dfs.append(df)
    concatenated_df = pd.concat(dfs, ignore_index=True)
    return concatenated_df

if __name__ == "__main__":
    base_path = '../results/'
    start_date, end_date = get_last_month_dates()
    action_name = sys.argv[1]

    files = get_date_range_files(base_path, start_date, end_date)
    concatenated_df = concatenate_csv(files)
    print(files)
    temp_csv_file = 'concatenated_data.csv'
    concatenated_df.to_csv(temp_csv_file, index=False, header=False)

    command = ['python', '4.py', temp_csv_file, "--action_name", action_name]
    print(f"Executing command: {' '.join(command)}")
    call(command)

    os.remove(temp_csv_file)
