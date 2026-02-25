import json
import csv
import os
import sqlite3
from kafka import KafkaConsumer
from datetime import datetime

CSV_FILE = "data/trajets.csv"
DB_FILE  = "data/smart_delivery.db"

def init_csv():
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp","driver_id","nom","lat","lon",
                             "battery","statut","vitesse_kmh"])
        print(f"📄 Fichier CSV créé : {CSV_FILE}")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS positions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            driver_id TEXT,
            nom       TEXT,
            lat       REAL,
            lon       REAL,
            battery   REAL,
            statut    TEXT,
            vitesse   REAL
        )
    ''')
    conn.commit()
    return conn

def main():
    init_csv()
    conn = init_db()
    
    consumer = KafkaConsumer(
        'delivery-locations',
        bootstrap_servers='localhost:9092',
        group_id='archiver-group',        # Group différent du alert-service !
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    print("💾 Archiveur démarré — Sauvegarde en cours...\n")
    count = 0

    for message in consumer:
        data = message.value
        row = [
            data['timestamp'], data['driver_id'], data['nom'],
            data['lat'], data['lon'], data['battery'],
            data['statut'], data['vitesse_kmh']
        ]

        # Sauvegarde CSV
        with open(CSV_FILE, 'a', newline='') as f:
            csv.writer(f).writerow(row)

        # Sauvegarde SQLite
        conn.execute('''
            INSERT INTO positions (timestamp,driver_id,nom,lat,lon,battery,statut,vitesse)
            VALUES (?,?,?,?,?,?,?,?)
        ''', row)
        conn.commit()

        count += 1
        print(f"💾 [{count}] Archivé — {data['driver_id']} | "
              f"Batterie: {data['battery']}% | {data['timestamp']}")

if __name__ == "__main__":
    main()