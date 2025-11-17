"""
Kafka Consumer - Estación Meteorológica
Consume datos del broker Kafka y grafica telemetría en tiempo real
Servidor: iot.redesuvg.cloud:9092
Topic: 21486
"""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import matplotlib.pyplot as plt
from datetime import datetime
from collections import deque

# Configuración del servidor Kafka
KAFKA_SERVER = 'iot.redesuvg.cloud:9092'
TOPIC = "21486"
GROUP_ID = "estacion-meteorologica-consumer"

# Listas para almacenar datos (mantienen el histórico)
all_temp = []
all_hume = []
all_wind = []
timestamps = []

# Mapeo de direcciones para graficar
DIRECCIONES_MAP = {'N': 0, 'NE': 45, 'E': 90, 'SE': 135, 'S': 180, 'SO': 225, 'O': 270, 'NO': 315}

# Mapeo inverso para decodificar dirección del viento (3 bits)
WIND_CODE_MAP = {0: 'N', 1: 'NO', 2: 'O', 3: 'SO', 4: 'S', 5: 'SE', 6: 'E', 7: 'NE'}


def decodificar_datos(data_bytes):
    """
    Decodifica 3 bytes (24 bits) a datos de sensores
    
    Formato de 24 bits:
    - Bits 23-10 (14 bits): Temperatura en centésimas [0-11000]
    - Bits 9-3 (7 bits): Humedad [0-100]
    - Bits 2-0 (3 bits): Dirección del viento [0-7]
    
    Retorna: dict con temperatura, humedad, direccion_viento
    """
    # Convertir bytes a entero de 24 bits
    packed = int.from_bytes(data_bytes, byteorder='big')
    
    # Extraer campos usando máscaras de bits
    temp_int = (packed >> 10) & 0x3FFF  # 14 bits (máscara: 0x3FFF = 16383)
    hum_int = (packed >> 3) & 0x7F      # 7 bits (máscara: 0x7F = 127)
    wind_code = packed & 0x07            # 3 bits (máscara: 0x07 = 7)
    
    # Convertir a valores reales
    temperatura = temp_int / 100.0  # De centésimas a decimal
    humedad = hum_int
    direccion = WIND_CODE_MAP.get(wind_code, 'N')
    
    return {
        'temperatura': temperatura,
        'humedad': humedad,
        'direccion_viento': direccion
    }


def create_consumer():
    """Crea y configura un consumidor Kafka"""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            auto_offset_reset='latest',  # Solo leer mensajes nuevos
            enable_auto_commit=True,
            group_id=GROUP_ID,
            # Sin deserializador - recibimos bytes directamente
            consumer_timeout_ms=1000  # Timeout para actualización de gráfica
        )
        print(f"✓ Consumer conectado a {KAFKA_SERVER}")
        print(f"✓ Suscrito al topic: {TOPIC}")
        print(f"✓ Formato: Payload de 3 bytes (24 bits)")
        return consumer
    except Exception as e:
        print(f"✗ Error al crear consumer: {e}")
        return None


def procesar_mensaje(message):
    """Procesa un mensaje recibido y actualiza las listas de datos"""
    try:
        # El valor del mensaje son bytes directamente
        data_bytes = message.value
        
        # Verificar que sean exactamente 3 bytes
        if len(data_bytes) != 3:
            print(f"✗ Payload inválido: {len(data_bytes)} bytes (esperado: 3)")
            return False
        
        # Decodificar los 3 bytes
        data = decodificar_datos(data_bytes)
        
        # Extraer datos
        temperatura = data['temperatura']
        humedad = data['humedad']
        direccion = data['direccion_viento']
        
        # Agregar a las listas
        all_temp.append(temperatura)
        all_hume.append(humedad)
        all_wind.append(DIRECCIONES_MAP.get(direccion, 0))
        timestamps.append(datetime.now())
        
        # Mostrar datos recibidos
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Mensaje recibido")
        print(f"  Payload: {len(data_bytes)} bytes = {len(data_bytes) * 8} bits")
        print(f"  Hex: {data_bytes.hex()}")
        print(f"  Temperatura: {temperatura}°C")
        print(f"  Humedad: {humedad}%")
        print(f"  Dirección del viento: {direccion}")
        print(f"  Total de lecturas: {len(all_temp)}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error al procesar mensaje: {e}")
        return False


def graficar_datos():
    """Crea/actualiza las gráficas con los datos actuales"""
    if len(all_temp) == 0:
        return
    
    plt.ion()  # Modo interactivo
    
    # Crear figura si no existe o si fue cerrada
    if not plt.fignum_exists(1):
        plt.figure(1, figsize=(12, 10))
    
    plt.clf()  # Limpiar figura
    
    indices = list(range(1, len(all_temp) + 1))
    
    # Subplot 1: Temperatura
    plt.subplot(3, 1, 1)
    plt.plot(indices, all_temp, 'r-', linewidth=2, marker='o', markersize=5)
    plt.title('Temperatura (°C)', fontsize=12, fontweight='bold')
    plt.ylabel('°C')
    plt.xlabel('Número de Lectura')
    plt.grid(True, alpha=0.3)
    plt.ylim(0, 110)
    
    # Subplot 2: Humedad
    plt.subplot(3, 1, 2)
    plt.plot(indices, all_hume, 'b-', linewidth=2, marker='s', markersize=5)
    plt.title('Humedad Relativa (%)', fontsize=12, fontweight='bold')
    plt.ylabel('%')
    plt.xlabel('Número de Lectura')
    plt.grid(True, alpha=0.3)
    plt.ylim(0, 100)
    
    # Subplot 3: Dirección del Viento
    plt.subplot(3, 1, 3)
    plt.plot(indices, all_wind, 'g-', linewidth=2, marker='^', markersize=5)
    plt.title('Dirección del Viento', fontsize=12, fontweight='bold')
    plt.ylabel('Dirección')
    plt.xlabel('Número de Lectura')
    plt.yticks([0, 45, 90, 135, 180, 225, 270, 315], 
               ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO'])
    plt.grid(True, alpha=0.3)
    plt.ylim(-10, 360)
    
    plt.tight_layout()
    plt.suptitle(f'Telemetría en Tiempo Real - Estación Meteorológica (Total: {len(all_temp)} lecturas)', 
                 fontsize=14, fontweight='bold', y=0.998)
    
    plt.draw()
    plt.pause(0.1)  # Pausa corta para actualizar la ventana


def iniciar_consumer():
    """Inicia el consumer y visualiza datos en tiempo real"""
    print("=" * 70)
    print("Kafka Consumer - Estación Meteorológica (Payload 3 bytes)")
    print("=" * 70)
    print(f"Servidor: {KAFKA_SERVER}")
    print(f"Topic: {TOPIC}")
    print(f"Group ID: {GROUP_ID}")
    print(f"Formato: 24 bits = Temp(14b) + Humedad(7b) + Viento(3b)")
    print("=" * 70)
    
    # Crear consumer
    consumer = create_consumer()
    if not consumer:
        print("No se pudo crear el consumer. Terminando...")
        return
    
    print("\nIniciando consumo de datos...")
    print("Esperando mensajes cada ~15 segundos...")
    print("La gráfica se actualizará automáticamente con cada nuevo mensaje")
    print("Presiona Ctrl+C para detener\n")
    
    try:
        while True:
            # Poll para obtener mensajes nuevos (timeout de 1 segundo)
            messages = consumer.poll(timeout_ms=1000)
            
            # Procesar cada mensaje recibido
            for topic_partition, msgs in messages.items():
                for message in msgs:
                    # Procesar y guardar el mensaje
                    if procesar_mensaje(message):
                        # Actualizar la gráfica con los nuevos datos
                        graficar_datos()
            
            # Pequeña pausa para no saturar el CPU
            plt.pause(0.1)
                        
    except KeyboardInterrupt:
        print("\n\n✓ Consumer detenido por el usuario")
    except Exception as e:
        print(f"\n✗ Error inesperado: {e}")
    finally:
        # Cerrar el consumer
        if consumer:
            consumer.close()
            print("✓ Consumer cerrado correctamente")
            print(f"✓ Total de lecturas procesadas: {len(all_temp)}")
        
        # Mantener la última gráfica visible
        if len(all_temp) > 0:
            print("\nLa última gráfica permanecerá abierta. Cierra la ventana manualmente.")
            plt.ioff()
            plt.show()


if __name__ == "__main__":
    iniciar_consumer()