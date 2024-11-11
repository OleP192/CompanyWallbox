import requests
import sqlite3
import csv
import os
from datetime import datetime

# Replace with your actual credentials and API endpoints
MERCEDES_API_URL = "https://api.mercedes-benz.com/vehicledata/v2/vehicles/{vehicle_id}/odometer"
MERCEDES_API_KEY = "your_mercedes_api_key"
VEHICLE_ID = "your_vehicle_id"

EASEE_API_URL = "https://api.easee.cloud/api/sites/{site_id}/chargers/{charger_id}"
EASEE_API_KEY = "your_easee_api_key"
SITE_ID = "your_site_id"
CHARGER_ID = "your_charger_id"

DATABASE_PATH = 'data.db'
CSV_DIR = '/tmp/'

def get_mercedes_km_status():
    headers = {
        "Authorization": f"Bearer {MERCEDES_API_KEY}",
        "Content-Type": "application/json"
    }
    response = requests.get(MERCEDES_API_URL.format(vehicle_id=VEHICLE_ID), headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get("odometer")
    else:
        print(f"Failed to get KM status: {response.status_code}")
        return None

def get_easee_kwh_usage():
    headers = {
        "Authorization": f"Bearer {EASEE_API_KEY}",
        "Content-Type": "application/json"
    }
    response = requests.get(EASEE_API_URL.format(site_id=SITE_ID, charger_id=CHARGER_ID), headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get("totalKwh")
    else:
        print(f"Failed to get kWh usage: {response.status_code}")
        return None

def write_to_database(km_status, kwh_usage):
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, date TEXT, km_status REAL, kwh_usage REAL)''')
    cursor.execute('''INSERT INTO data (date, km_status, kwh_usage) VALUES (?, ?, ?)''', (datetime.now().date(), km_status, kwh_usage))
    conn.commit()
    conn.close()

def create_or_update_csv(km_status, kwh_usage):
    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(CSV_DIR, f'charging_data_{today_str}.csv')
    
    file_exists = os.path.isfile(csv_path)
    
    with open(csv_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["Date", "KM Status", "kWh Usage"])
        writer.writerow([datetime.now(), km_status, kwh_usage])

def main():
    km_status = get_mercedes_km_status()
    kwh_usage = get_easee_kwh_usage()

    if km_status is not None and kwh_usage is not None:
        write_to_database(km_status, kwh_usage)
        create_or_update_csv(km_status, kwh_usage)
        print("Data written to database and CSV file created/updated successfully.")
    else:
        print("Failed to gather data.")

if __name__ == "__main__":
    main()
