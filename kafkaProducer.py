"""
Kafka Producer - Estación Meteorológica
Genera datos de sensores y los publica al broker Kafka
Servidor: iot.redesuvg.cloud:9092
Topic: 21486
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
from datetime import datetime
from sensorSimulator import generar_data

# Configuración del servidor Kafka
KAFKA_SERVER = 'iot.redesuvg.cloud:9092'
TOPIC = "21486"
INTERVALO = 15  # segundos entre envíos


def create_producer():
    """Crea y configura un productor Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Esperar confirmación de todas las réplicas
            retries=3,
            request_timeout_ms=30000
        )
        print(f"Producer conectado a {KAFKA_SERVER}")
        return producer
    except Exception as e:
        print(f"Error al crear producer: {e}")
        return None


def enviar_lectura(producer, topic):
    """Genera una lectura de sensores y la envía al broker"""
    try:
        # Generar datos de los sensores
        data = generar_data()
        
        # Enviar al broker Kafka
        future = producer.send(topic, value=data)
        record_metadata = future.get(timeout=10)
        
        # Mostrar confirmación
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Mensaje enviado exitosamente")
        print(f"  Topic: {record_metadata.topic}")
        print(f"  Partición: {record_metadata.partition}")
        print(f"  Offset: {record_metadata.offset}")
        print(f"  Datos: {json.dumps(data, indent=2)}")
        
        return True
        
    except KafkaError as e:
        print(f"Error de Kafka: {e}")
        return False
    except Exception as e:
        print(f"Error al enviar lectura: {e}")
        return False


def iniciar_productor():
    """Inicia el producer y envía lecturas periódicamente"""
    print("=" * 70)
    print("Estacion Meteorologica")
    print("=" * 70)
    print(f"Servidor: {KAFKA_SERVER}")
    print(f"Topic: {TOPIC}")
    print(f"Intervalo: {INTERVALO} segundos")
    print("=" * 70)
    
    # Crear producer
    producer = create_producer()
    if not producer:
        print("No se pudo crear el producer. Terminando...")
        return
    
    print("\nIniciando envío de datos...")
    print("Ctrl+C para detener el envio de datos al server\n")
    
    contador = 0
    try:
        while True:
            contador += 1
            print(f"\n--- Lectura #{contador} ---")
            
            # Enviar lectura
            enviar_lectura(producer, TOPIC)
            
            # Esperar antes de la siguiente lectura
            print(f"Esperando {INTERVALO} segundos para la proxima lectura...")
            time.sleep(INTERVALO)
            
    except KeyboardInterrupt:
        print("\n\nProductor detenido por el usuario")
    except Exception as e:
        print(f"\n Error inesperado: {e}")
    finally:
        # Cerrar el producer
        if producer:
            producer.flush()
            producer.close()
            print("Producer cerrado correctamente")


if __name__ == "__main__":
    iniciar_productor()

