import json
from kafka import KafkaConsumer, KafkaProducer

def main():
    consumer = KafkaConsumer(
        'delivery-locations',
        bootstrap_servers='localhost:9092',
        group_id='alert-service-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    print("🚨 Service d'Alertes démarré — En attente de messages...\n")

    for message in consumer:
        data = message.value

        if data['battery'] < 10:
            print(f"🔴 ALERTE CRITIQUE — {data['driver_id']} ({data['nom']}) "
                  f"| Batterie à {data['battery']}% | Position: ({data['lat']}, {data['lon']})")
            # Envoie vers le topic urgent-maintenance
            producer.send('urgent-maintenance', key=data['driver_id'], value=data)
            producer.flush()

        elif data['battery'] < 15:
            print(f"🟡 ALERTE FAIBLE — {data['driver_id']} ({data['nom']}) "
                  f"| Batterie à {data['battery']}% | Retour en base recommandé")

if __name__ == "__main__":
    main()