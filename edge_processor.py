import paho.mqtt.client as mqtt
import redis
import requests
import json
import time

# ==========================================
# 1. CONFIGURACIÓN DEL ENTORNO
# ==========================================
# Mosquitto (El Broker local)
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "yaku/telemetria/pond/#" # El '#' escucha cualquier estanque

# Redis (La Base de Datos en Memoria)
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_QUEUE = "telemetry_queue"

# API DESTINO (Tu API Gateway o Monolito)
# Cámbiar a la URL de Azure cuando vayas a producción
API_URL = "http://localhost:8080/api/v1/telemetry/manual-ingest" 

# Inicializar conexión a Redis
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    print("✅ Conectado a Redis")
except Exception as e:
    print(f"❌ Error conectando a Redis: {e}")
    exit(1)

# ==========================================
# 2. EL RECEPTOR (MQTT -> REDIS)
# ==========================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Conectado a Mosquitto MQTT en {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"❌ Fallo al conectar a MQTT. Código: {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    print(f"\n📥 [MQTT] Recibido en tópico '{msg.topic}': {payload}")
    
    try:
        data = json.loads(payload)
        
        # El tópico debería ser algo como: "yaku/telemetria/pond/1"
        # Extraemos el ID del estanque (ej. '1') haciendo un split
        partes_topico = msg.topic.split('/')
        
        # Validación básica para asegurar que el tópico tiene el formato correcto
        if len(partes_topico) >= 4 and partes_topico[2] == "pond":
            pond_id = int(partes_topico[3])
            
            # Inyectamos el pondId requerido por el Backend
            data["pondId"] = pond_id
            
            # Empujar a la cola izquierda de Redis (FIFO)
            redis_client.lpush(REDIS_QUEUE, json.dumps(data))
            print(f"💾 [REDIS] Dato guardado en la cola local para Pond {pond_id}")
        else:
            print(f"⚠️ [MQTT] Tópico ignorado o formato inválido: {msg.topic}")

    except json.JSONDecodeError:
        print("⚠️ [ERROR] El payload no es un JSON válido")
    except ValueError:
        print("⚠️ [ERROR] ID de estanque no es un número válido")

# Configurar el cliente MQTT
mqtt_client = mqtt.Client(client_id="YakuEdgeProcessor")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# ==========================================
# 3. EL SINCRONIZADOR (REDIS -> NUBE)
# ==========================================
def sync_to_cloud():
    print("☁️ Iniciando el motor de sincronización hacia la API...")
    while True:
        try:
            # Extraer el dato más antiguo de la cola derecha (RPOP)
            dato_crudo = redis_client.rpop(REDIS_QUEUE)
            
            if dato_crudo:
                data = json.loads(dato_crudo)
                print(f"🚀 [API] Enviando dato a la nube: {data}")
                
                # Disparar hacia la API (con un límite de 5 segundos de espera)
                response = requests.post(API_URL, json=data, timeout=5)
                
                if response.status_code in (200, 201):
                    print("✅ [API] Subida exitosa.")
                else:
                    print(f"❌ [API] Rechazó el dato (HTTP {response.status_code}). Devolviendo a la cola...")
                    # Si la API falla pero hay internet, devolvemos a la cola derecha para reintentar
                    redis_client.rpush(REDIS_QUEUE, dato_crudo) 
            else:
                # Si la cola está vacía, dormimos 1 segundo para no saturar el CPU de la laptop
                time.sleep(1)
                
        except requests.exceptions.RequestException as e:
            print(f"🔌 [RED] Sin internet o API caída: {e}")
            if dato_crudo:
                print("💾 [REDIS] Rescatando el dato y devolviéndolo a la cola...")
                redis_client.rpush(REDIS_QUEUE, dato_crudo)
            # Si se cae el internet, esperamos más tiempo (5s) antes de volver a estresar la red
            time.sleep(5) 
        except Exception as e:
            print(f"🔥 [SISTEMA] Error inesperado: {e}")
            time.sleep(2)

# ==========================================
# 4. ARRANQUE DEL SISTEMA
# ==========================================
if __name__ == "__main__":
    # Arrancar MQTT en un hilo secundario (no bloquea el código)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
    
    # Arrancar el sincronizador de nube en el hilo principal
    try:
        sync_to_cloud()
    except KeyboardInterrupt:
        print("\n🛑 Apagando el Nodo Edge...")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("Apagado seguro completado.")