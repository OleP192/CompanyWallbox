import sqlite3
import time
import requests
import csv
from datetime import datetime

# API-Endpunkte und Authentifizierung
BMW_API_URL = "https://b2vapi.bmwgroup.com/webapi/v1/user/vehicles/"
VIESSMANN_API_URL = "https://api.viessmann.com/iot/v1/equipment/installations/{installationId}/gateways/{gatewayId}/devices/{deviceId}/features/"
BMW_AUTH_TOKEN = "YOUR_BMW_AUTH_TOKEN"
BMW_VEHICLE_ID = "YOUR_BMW_VEHICLE_ID"

# Datenbank initialisieren
def initialize_database():
    connection = sqlite3.connect('charging_log.db')
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS charging_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        km_stand INTEGER,
                        kwh_used REAL,
                        date TEXT
                      )''')
    connection.commit()
    connection.close()

# Abfrage des Kilometerstands vom BMW
def get_bmw_kilometer_stand():
    headers = {
        'Authorization': f'Bearer {BMW_AUTH_TOKEN}'
    }
    response = requests.get(f"{BMW_API_URL}{BMW_VEHICLE_ID}/status", headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get("mileage", 0)
    else:
        print(f"Fehler beim Abrufen des Kilometerstands: {response.status_code}")
        return None

# Abfrage der verbrauchten kWh von der Wallbox
def get_viessmann_kwh_consumed():
    response = requests.get(f"{VIESSMANN_API_URL}electricity/charging")
    if response.status_code == 200:
        data = response.json()
        return data.get("properties", {}).get("totalEnergy", {}).get("value", 0)
    else:
        print(f"Fehler beim Abrufen der kWh-Daten: {response.status_code}")
        return None

# Daten in die Datenbank speichern
def log_charging_data(km_stand, kwh_used):
    connection = sqlite3.connect('charging_log.db')
    cursor = connection.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    date = datetime.now().strftime('%Y-%m-%d')
    cursor.execute("INSERT INTO charging_log (timestamp, km_stand, kwh_used, date) VALUES (?, ?, ?, ?)",
                   (timestamp, km_stand, kwh_used, date))
    connection.commit()
    connection.close()
    export_to_csv()

# Exportieren der Datenbankinhalte in eine CSV-Datei
def export_to_csv():
    connection = sqlite3.connect('charging_log.db')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM charging_log")
    rows = cursor.fetchall()
    connection.close()
    
    with open('/tmp/charging_log.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['ID', 'Timestamp', 'KM Stand', 'kWh Used', 'Date'])
        csvwriter.writerows(rows)

# Hauptfunktion zur Abfrage und Speicherung
def main():
    initialize_database()
    charging_active = False
    initial_kwh = None

    while True:
        # Check, ob ein Ladevorgang aktiv ist
        current_kwh = get_viessmann_kwh_consumed()
        if current_kwh is None:
            time.sleep(60)
            continue

        if current_kwh > 0 and not charging_active:
            # Ladevorgang beginnt
            charging_active = True
            initial_kwh = current_kwh
            km_stand = get_bmw_kilometer_stand()
            print("Ladevorgang erkannt. KM-Stand erfasst.")
        elif current_kwh == 0 and charging_active:
            # Ladevorgang beendet
            charging_active = False
            final_kwh = get_viessmann_kwh_consumed()
            if initial_kwh is not None and final_kwh is not None:
                kwh_used = final_kwh - initial_kwh
                log_charging_data(km_stand, kwh_used)
                print("Ladevorgang beendet. Daten wurden gespeichert.")
            initial_kwh = None
        
        # Wartezeit von einer Minute
        time.sleep(60)

if __name__ == "__main__":
    main()
