from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time
import csv
import os
from datetime import datetime

start_url = "https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html"

# Create directory based on the current month
base_directory = "../results/"
today_date = datetime.now().strftime("%Y-%m-%d")
month = datetime.now().strftime("%Y-%m")

# Define the directory to store this month's files
monthly_directory = os.path.join(base_directory, month)

# Ensure the directory exists
if not os.path.exists(monthly_directory):
    os.makedirs(monthly_directory)

with webdriver.Firefox() as driver:
    wait = WebDriverWait(driver, 10)
    driver.get(start_url)

    time.sleep(3)

    # Retrieve the list of stocks
    commodities = driver.find_elements(By.XPATH, "/html/body/main/section/div/div/div/ul/li/div/section/div/article/section[2]/ul[2]/li[1]/div/section/table/tbody/tr")

    # Filename for today's data
    file_name = f"stocks_data_{today_date}.csv"
    file_path = os.path.join(monthly_directory, file_name)

    # Open the CSV file
    with open(file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        for stock in commodities:
            # Retrieve the information for each stock
            values = stock.find_elements(By.XPATH, "td")
            list_of_values = [x.text for x in values]

            # Extract necessary values
            if len(list_of_values) >= 7:  # Check if there are enough columns
                name = list_of_values[0].replace('.', '').replace(',', '.')  # Name of the action
                last_quote = list_of_values[1].replace('.', '').replace(',', '.')  # Last quotation
                session_high = list_of_values[5].replace('.', '').replace(',', '.')  # Maximum of the session
                session_low = list_of_values[6].replace('.', '').replace(',', '.')  # Minimum of the session
                date_time = list_of_values[-1]  # Date-time, could be either time or date
                
                # Write the data row to the CSV file
                writer.writerow([name, last_quote, session_high, session_low, date_time])

print("Data collection and storage completed successfully.")
