import os
from datetime import datetime
from subprocess import call

def find_monthly_files(base_path):
    year_month = datetime.now().strftime('%Y-%m')
    directory = os.path.join(base_path, year_month)
    # List all CSV files in the month directory
    return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.csv') and os.path.isfile(os.path.join(directory, file))]

base_path = '../results/'
files = find_monthly_files(base_path)
print(files)

# Call the MRJob script with the selected files
command = ['python', '2.py'] + files
call(command)
