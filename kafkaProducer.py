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

# Mapeo de direcciones del viento a 3 bits (0-7)
WIND_MAP = {'N': 0, 'NO': 1, 'O': 2, 'SO': 3, 'S': 4, 'SE': 5, 'E': 6, 'NE': 7}


def codificar_datos(temperatura, humedad, direccion_viento):
    """
    Codifica los datos de los sensores en 3 bytes (24 bits)
    
    Formato de 24 bits:
    - Bits 23-10 (14 bits): Temperatura en centésimas [0-11000]
    - Bits 9-3 (7 bits): Humedad [0-100]
    - Bits 2-0 (3 bits): Dirección del viento [0-7]
    
    Retorna: bytes (3 bytes)
    """
    # Temperatura en centésimas (14 bits)
    temp_int = int(round(temperatura * 100))
    temp_int = max(0, min(16383, temp_int))  # Limitar a 14 bits (0-16383)
    
    # Humedad (7 bits)
    hum_int = max(0, min(127, humedad))  # Limitar a 7 bits (0-127)
    
    # Dirección del viento (3 bits)
    wind_code = WIND_MAP.get(direccion_viento, 0)  # 0-7
    
    # Empaquetar en 24 bits: [temp(14)][ hum(7) ][ wind(3) ]
    packed = (temp_int << 10) | (hum_int << 3) | wind_code
    
    # Convertir a 3 bytes (big-endian)
    return packed.to_bytes(3, byteorder='big')


def decodificar_datos_debug(data_bytes):
    """
    Decodifica 3 bytes para verificar (solo para debug)
    Retorna: dict con temperatura, humedad, direccion_viento
    """
    # Convertir bytes a entero
    packed = int.from_bytes(data_bytes, byteorder='big')
    
    # Extraer campos
    temp_int = (packed >> 10) & 0x3FFF  # 14 bits
    hum_int = (packed >> 3) & 0x7F      # 7 bits
    wind_code = packed & 0x07            # 3 bits
    
    # Convertir a valores originales
    temperatura = temp_int / 100.0
    humedad = hum_int
    
    # Mapeo inverso de dirección
    wind_map_inv = {v: k for k, v in WIND_MAP.items()}
    direccion = wind_map_inv.get(wind_code, 'N')
    
    return {
        'temperatura': temperatura,
        'humedad': humedad,
        'direccion_viento': direccion
    }


def create_producer():
    """Crea y configura un productor Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            # Sin serializador - enviamos bytes directamente
            acks='all',
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
        temperatura = data['temperatura']
        humedad = data['humedad']
        direccion = data['direccion_viento']
        
        # Codificar en 3 bytes (24 bits)
        payload_bytes = codificar_datos(temperatura, humedad, direccion)
        
        # Enviar al broker Kafka (3 bytes)
        future = producer.send(topic, value=payload_bytes)
        record_metadata = future.get(timeout=10)
        
        # Decodificar para verificación (debug)
        data_verificado = decodificar_datos_debug(payload_bytes)
        
        # Mostrar confirmación
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Mensaje enviado exitosamente")
        print(f"  Topic: {record_metadata.topic}")
        print(f"  Partición: {record_metadata.partition}")
        print(f"  Offset: {record_metadata.offset}")
        print(f"  Payload: {len(payload_bytes)} bytes = {len(payload_bytes) * 8} bits")
        print(f"  Hex: {payload_bytes.hex()}")
        print(f"  Datos originales:")
        print(f"    Temperatura: {temperatura}°C")
        print(f"    Humedad: {humedad}%")
        print(f"    Dirección: {direccion}")
        print(f"  Datos codificados:")
        temp_int = int(round(temperatura * 100))
        hum_int = humedad
        wind_code = WIND_MAP.get(direccion, 0)
        print(f"    Temp int: {temp_int} (14 bits: {bin(temp_int)[2:].zfill(14)})")
        print(f"    Hum int: {hum_int} (7 bits: {bin(hum_int)[2:].zfill(7)})")
        print(f"    Wind code: {wind_code} (3 bits: {bin(wind_code)[2:].zfill(3)})")
        packed = (temp_int << 10) | (hum_int << 3) | wind_code
        print(f"    Packed 24 bits: {bin(packed)[2:].zfill(24)}")
        print(f"  Datos decodificados (verificación):")
        print(f"    Temperatura: {data_verificado['temperatura']}°C")
        print(f"    Humedad: {data_verificado['humedad']}%")
        print(f"    Dirección: {data_verificado['direccion_viento']}")
        
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
    print("Estacion Meteorologica - Payload Comprimido (3 bytes)")
    print("=" * 70)
    print(f"Servidor: {KAFKA_SERVER}")
    print(f"Topic: {TOPIC}")
    print(f"Intervalo: {INTERVALO} segundos")
    print(f"Formato: 24 bits = Temp(14b) + Humedad(7b) + Viento(3b)")
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

