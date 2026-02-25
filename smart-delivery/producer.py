import json
import time
import random
import math
from kafka import KafkaProducer

# --- Simulation de livreurs sur Dakar ---
# Zones de Dakar avec coordonnées GPS réelles
ZONES_DAKAR = {
    "Plateau":      (14.6937, -17.4441),
    "Médina":       (14.6892, -17.4580),
    "Yoff":         (14.7645, -17.4677),
    "Almadies":     (14.7470, -17.5140),
    "Ouakam":       (14.7280, -17.4980),
    "Mermoz":       (14.7100, -17.4750),
    "Liberté":      (14.7050, -17.4600),
    "Grand Dakar":  (14.7200, -17.4400),
    "Pikine":       (14.7500, -17.3900),
    "Guédiawaye":   (14.7700, -17.3800),
}

LIVREURS = [
    {"id": "DRV_001", "nom": "Moussa Diallo",   "zone_depart": "Plateau"},
    {"id": "DRV_002", "nom": "Fatou Sow",        "zone_depart": "Médina"},
    {"id": "DRV_003", "nom": "Ibrahima Ndiaye",  "zone_depart": "Yoff"},
    {"id": "DRV_004", "nom": "Aminata Fall",     "zone_depart": "Pikine"},
    {"id": "DRV_005", "nom": "Omar Ba",          "zone_depart": "Almadies"},
]

class SimulateurLivreur:
    def __init__(self, livreur_info):
        self.id = livreur_info["id"]
        self.nom = livreur_info["nom"]
        zone = ZONES_DAKAR[livreur_info["zone_depart"]]
        self.lat = zone[0] + random.uniform(-0.005, 0.005)
        self.lon = zone[1] + random.uniform(-0.005, 0.005)
        self.battery = random.uniform(60, 100)
        self.vitesse = random.uniform(0.0001, 0.0004)
        self.direction = random.uniform(0, 2 * math.pi)
        self.statut = "en_livraison"

    def update(self):
        # Changement de direction aléatoire (simulation de rues)
        self.direction += random.uniform(-0.5, 0.5)
        
        # Déplacement
        self.lat += math.cos(self.direction) * self.vitesse
        self.lon += math.sin(self.direction) * self.vitesse

        # Rester dans les limites de Dakar
        self.lat = max(14.65, min(14.80, self.lat))
        self.lon = max(-17.55, min(-17.35, self.lon))

        # Décharge batterie (entre 0.05% et 0.2% par update)
        self.battery -= random.uniform(0.05, 0.2)
        self.battery = max(0, self.battery)

        # Déterminer le statut
        if self.battery < 10:
            self.statut = "critique"
        elif self.battery < 15:
            self.statut = "alerte"
        else:
            self.statut = "en_livraison"

        return {
            "driver_id": self.id,
            "nom": self.nom,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "battery": round(self.battery, 2),
            "statut": self.statut,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "vitesse_kmh": round(self.vitesse * 100000 * 3.6, 1)
        }


def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    livreurs = [SimulateurLivreur(l) for l in LIVREURS]
    print("🚴 Démarrage du producteur Smart Delivery - Dakar")
    print(f"   {len(livreurs)} livreurs actifs\n")

    cycle = 0
    while True:
        cycle += 1
        for livreur in livreurs:
            data = livreur.update()
            
            # Envoi vers Kafka avec driver_id comme clé de partition
            producer.send(
                topic='delivery-locations',
                key=data['driver_id'],
                value=data
            )

            # Affichage console
            icone = "🔴" if data['statut'] == "critique" else "🟡" if data['statut'] == "alerte" else "🟢"
            print(f"{icone} [{data['driver_id']}] {data['nom']} | "
                  f"Batterie: {data['battery']}% | "
                  f"Position: ({data['lat']}, {data['lon']}) | "
                  f"Statut: {data['statut']}")

        producer.flush()
        print(f"--- Cycle {cycle} envoyé ---\n")
        time.sleep(5)


if __name__ == "__main__":
    main()