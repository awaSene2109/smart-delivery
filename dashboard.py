import json
import threading
import sqlite3
from flask import Flask, render_template_string, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

# Stockage en mémoire des dernières positions
derniere_position = {}

# ── Thread Kafka ──────────────────────────────────────────────
def lire_kafka():
    consumer = KafkaConsumer(
        'delivery-locations',
        bootstrap_servers='localhost:9092',
        group_id='dashboard-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    for message in consumer:
        data = message.value
        driver_id = data['driver_id']
        
        if driver_id not in derniere_position:
            derniere_position[driver_id] = {"trail": []}
        
        derniere_position[driver_id].update(data)
        trail = derniere_position[driver_id]["trail"]
        trail.append({"lat": data["lat"], "lon": data["lon"]})
        if len(trail) > 30:  # Garder les 30 dernières positions
            trail.pop(0)

# ── Routes Flask ──────────────────────────────────────────────
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/positions')
def get_positions():
    return jsonify(list(derniere_position.values()))


# ── Template HTML + Leaflet.js ────────────────────────────────
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>Smart Delivery Dakar — Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body { font-family: 'Segoe UI', sans-serif; background:#0f172a; color:#e2e8f0; display:flex; flex-direction:column; height:100vh; }

    header { background:#1e293b; padding:12px 24px; display:flex; align-items:center; gap:16px; border-bottom:2px solid #334155; }
    header h1 { font-size:1.3rem; color:#38bdf8; }
    header span { font-size:0.85rem; color:#94a3b8; }
    #live-dot { width:10px; height:10px; background:#22c55e; border-radius:50%; animation:pulse 1.5s infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

    .main { display:flex; flex:1; overflow:hidden; }
    #map { flex:1; }

    .sidebar { width:320px; background:#1e293b; overflow-y:auto; padding:12px; display:flex; flex-direction:column; gap:10px; }
    .sidebar h2 { font-size:0.9rem; color:#94a3b8; text-transform:uppercase; letter-spacing:1px; margin-bottom:4px; }

    .card { background:#0f172a; border-radius:10px; padding:14px; border-left:4px solid #38bdf8; transition:all .3s; }
    .card.alerte  { border-color:#f59e0b; }
    .card.critique{ border-color:#ef4444; animation:flash 1s infinite; }
    @keyframes flash { 0%,100%{opacity:1} 50%{opacity:0.6} }

    .card-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; }
    .driver-name { font-weight:600; font-size:1rem; }
    .badge { padding:2px 8px; border-radius:20px; font-size:0.75rem; font-weight:600; }
    .badge.en_livraison { background:#166534; color:#86efac; }
    .badge.alerte       { background:#78350f; color:#fcd34d; }
    .badge.critique     { background:#7f1d1d; color:#fca5a5; }

    .stat { display:flex; justify-content:space-between; font-size:0.82rem; color:#94a3b8; margin:3px 0; }
    .stat span:last-child { color:#e2e8f0; font-weight:500; }

    .battery-bar { height:6px; background:#334155; border-radius:3px; margin:6px 0 3px; overflow:hidden; }
    .battery-fill { height:100%; border-radius:3px; transition:width .5s; }
    .battery-fill.high   { background:#22c55e; }
    .battery-fill.medium { background:#f59e0b; }
    .battery-fill.low    { background:#ef4444; }

    #stats-globales { background:#0f172a; border-radius:10px; padding:12px; }
    #stats-globales .sg { display:flex; justify-content:space-between; font-size:0.85rem; }

    footer { background:#1e293b; padding:6px 24px; font-size:0.75rem; color:#475569; border-top:1px solid #334155; }
  </style>
</head>
<body>
<header>
  <div id="live-dot"></div>
  <h1>🚴 Smart Delivery Dakar</h1>
  <span id="last-update">Connexion...</span>
</header>

<div class="main">
  <div id="map"></div>

  <aside class="sidebar">
    <h2>📡 Livreurs en temps réel</h2>
    <div id="stats-globales">
      <div class="sg"><span>Total actifs</span><span id="total">0</span></div>
      <div class="sg"><span>🟡 Alertes</span><span id="nb-alerte" style="color:#f59e0b">0</span></div>
      <div class="sg"><span>🔴 Critiques</span><span id="nb-critique" style="color:#ef4444">0</span></div>
    </div>
    <div id="drivers-list"></div>
  </aside>
</div>

<footer>Smart Delivery System • Dakar, Sénégal • Données simulées en temps réel via Apache Kafka</footer>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
// Initialisation carte centrée sur Dakar
const map = L.map('map').setView([14.7167, -17.4677], 13);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution:'© OpenStreetMap contributors', maxZoom:19
}).addTo(map);

const markers = {};
const trails  = {};
const COLORS  = {
  'DRV_001':'#38bdf8','DRV_002':'#a78bfa','DRV_003':'#34d399',
  'DRV_004':'#fb923c','DRV_005':'#f472b6'
};

function getBatteryClass(b) {
  return b > 30 ? 'high' : b > 15 ? 'medium' : 'low';
}

function createIcon(color, statut) {
  const pulse = statut === 'critique' ? 'animation:flash 1s infinite' : '';
  return L.divIcon({
    className:'',
    html: `<div style="width:28px;height:28px;background:${color};border:3px solid white;
           border-radius:50%;display:flex;align-items:center;justify-content:center;
           font-size:14px;box-shadow:0 2px 8px rgba(0,0,0,.5);${pulse}">🚴</div>`,
    iconSize:[28,28], iconAnchor:[14,14]
  });
}

async function updateDashboard() {
  try {
    const resp = await fetch('/api/positions');
    const drivers = await resp.json();

    let nbAlerte=0, nbCritique=0;
    const list = document.getElementById('drivers-list');
    list.innerHTML = '';

    drivers.forEach(d => {
      const color = COLORS[d.driver_id] || '#ffffff';

      // ── Marqueur ──────────────────────────────────────
      if (!markers[d.driver_id]) {
        markers[d.driver_id] = L.marker([d.lat, d.lon], {icon: createIcon(color, d.statut)})
          .addTo(map)
          .bindPopup('');
      } else {
        markers[d.driver_id].setLatLng([d.lat, d.lon]);
        markers[d.driver_id].setIcon(createIcon(color, d.statut));
      }
      markers[d.driver_id].setPopupContent(
        `<b>${d.nom}</b><br>🔋 ${d.battery}%<br>📍 ${d.lat}, ${d.lon}<br>⚡ ${d.vitesse_kmh} km/h`
      );

      // ── Trajet (trail) ─────────────────────────────────
      if (d.trail && d.trail.length > 1) {
        if (trails[d.driver_id]) map.removeLayer(trails[d.driver_id]);
        trails[d.driver_id] = L.polyline(
          d.trail.map(p => [p.lat, p.lon]),
          { color, weight:3, opacity:0.6, dashArray:'5,5' }
        ).addTo(map);
      }

      // ── Compteurs ─────────────────────────────────────
      if (d.statut === 'alerte')   nbAlerte++;
      if (d.statut === 'critique') nbCritique++;

      // ── Carte sidebar ──────────────────────────────────
      const bClass = getBatteryClass(d.battery);
      const card = document.createElement('div');
      card.className = `card ${d.statut !== 'en_livraison' ? d.statut : ''}`;
      card.innerHTML = `
        <div class="card-header">
          <span class="driver-name" style="color:${color}">● ${d.nom}</span>
          <span class="badge ${d.statut}">${d.statut.replace('_',' ')}</span>
        </div>
        <div class="battery-bar"><div class="battery-fill ${bClass}" style="width:${d.battery}%"></div></div>
        <div class="stat"><span>🔋 Batterie</span><span>${d.battery}%</span></div>
        <div class="stat"><span>📍 Latitude</span><span>${d.lat}</span></div>
        <div class="stat"><span>📍 Longitude</span><span>${d.lon}</span></div>
        <div class="stat"><span>⚡ Vitesse</span><span>${d.vitesse_kmh} km/h</span></div>
        <div class="stat"><span>🕐 Mise à jour</span><span>${d.timestamp ? d.timestamp.slice(11,19) : '-'}</span></div>
      `;
      card.onclick = () => map.flyTo([d.lat, d.lon], 16);
      list.appendChild(card);
    });

    document.getElementById('total').textContent = drivers.length;
    document.getElementById('nb-alerte').textContent = nbAlerte;
    document.getElementById('nb-critique').textContent = nbCritique;
    document.getElementById('last-update').textContent =
      'Dernière mise à jour : ' + new Date().toLocaleTimeString('fr-FR');

  } catch(e) { console.error(e); }
}

setInterval(updateDashboard, 3000);
updateDashboard();
</script>
</body>
</html>
"""

if __name__ == '__main__':
    # Démarrer le thread Kafka en arrière-plan
    t = threading.Thread(target=lire_kafka, daemon=True)
    t.start()
    print("🗺️  Dashboard disponible sur http://localhost:5000")
    app.run(debug=False, host='0.0.0.0', port=5000)