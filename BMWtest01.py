import sqlite3
import time
import requests
import csv
import logging
from datetime import datetime
from contextlib import closing

# Configuration
BMW_API_URL = "https://b2vapi.bmwgroup.com/webapi/v1/user/vehicles/"
VIESSMANN_API_URL = "https://api.viessmann.com/iot/v1/equipment/installations/{installationId}/gateways/{gatewayId}/devices/{deviceId}/features/"
BMW_AUTH_TOKEN = "YOUR_BMW_AUTH_TOKEN"
BMW_VEHICLE_ID = "YOUR_BMW_VEHICLE_ID"

# Setup logging
logging.basicConfig(filename='charging_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the database
def initialize_database():
    with closing(sqlite3.connect('charging_log.db')) as connection, connection.cursor() as cursor:
        cursor.execute('''CREATE TABLE IF NOT EXISTS charging_log (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            timestamp TEXT,
                            km_stand INTEGER,
                            kwh_used REAL,
                            date TEXT
                          )''')
        connection.commit()

# Fetch BMW mileage
def get_bmw_kilometer_stand():
    headers = {'Authorization': f'Bearer {BMW_AUTH_TOKEN}'}
    try:
        response = requests.get(f"{BMW_API_URL}{BMW_VEHICLE_ID}/status", headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("mileage", 0)
    except requests.RequestException as e:
        logging.error(f"Error fetching BMW mileage: {e}")
        return None

# Fetch Viessmann kWh consumed
def get_viessmann_kwh_consumed():
    try:
        response = requests.get(f"{VIESSMANN_API_URL}electricity/charging")
        response.raise_for_status()
        data = response.json()
        return data.get("properties", {}).get("totalEnergy", {}).get("value", 0)
    except requests.RequestException as e:
        logging.error(f"Error fetching Viessmann kWh data: {e}")
        return None

# Log charging data
def log_charging_data(km_stand, kwh_used):
    with closing(sqlite3.connect('charging_log.db')) as connection, connection.cursor() as cursor:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        date = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("INSERT INTO charging_log (timestamp, km_stand, kwh_used, date) VALUES (?, ?, ?, ?)",
                       (timestamp, km_stand, kwh_used, date))
        connection.commit()
    export_to_csv()

# Export data to CSV
def export_to_csv():
    with closing(sqlite3.connect('charging_log.db')) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT * FROM charging_log")
        rows = cursor.fetchall()

    with open('/tmp/charging_log.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['ID', 'Timestamp', 'KM Stand', 'kWh Used', 'Date'])
        csvwriter.writerows(rows)

# Main function
def main():
    initialize_database()
    charging_active = False
    initial_kwh = None

    while True:
        current_kwh = get_viessmann_kwh_consumed()
        if current_kwh is None:
            time.sleep(60)
            continue

        if current_kwh > 0 and not charging_active:
            charging_active = True
            initial_kwh = current_kwh
            km_stand = get_bmw_kilometer_stand()
            logging.info("Charging session started. KM stand recorded.")
        elif current_kwh == 0 and charging_active:
            charging_active = False
            final_kwh = get_viessmann_kwh_consumed()
            if initial_kwh is not None and final_kwh is not None:
                kwh_used = final_kwh - initial_kwh
                log_charging_data(km_stand, kwh_used)
                logging.info("Charging session ended. Data saved.")
            initial_kwh = None
        
        time.sleep(60)

if __name__ == "__main__":
    main()
