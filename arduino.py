import paho.mqtt.client as mqtt
import json
import time
import random

# Configuración hacia tu Mosquitto local
BROKER = "localhost"
PORT = 1883
TOPIC = "yaku/telemetria/pond/1"

client = mqtt.Client(client_id="ArduinoSimulador")

try:
    client.connect(BROKER, PORT)
    print("🤖 Fake Arduino conectado a MQTT. Enviando datos cada 3 segundos...")
    print("Presiona Ctrl+C para detenerlo.\n")
    
    while True:
        # Generar datos aleatorios simulando los sensores físicos de agua
        payload = {
            "temperature": round(random.uniform(20.0, 35.0), 2),
            "ph": round(random.uniform(6.5, 8.5), 2),
            "turbidity": round(random.uniform(0.5, 5.0), 2)
        }
        
        # Publicar el mensaje en el broker
        client.publish(TOPIC, json.dumps(payload))
        print(f"📡 [SIMULADOR] Publicado en {TOPIC}: {payload}")
        
        time.sleep(3)

except ConnectionRefusedError:
    print("❌ Error: No se pudo conectar a Mosquitto. ¿Está corriendo Docker?")
except KeyboardInterrupt:
    print("\n🛑 Apagando el simulador...")
    client.disconnect()