"""
Lab 7 - Estación Meteorológica
Simulador de sensores con lecturas aleatorias
"""

import random
import json

# Configuración de los sensores
DIRECCIONES_VIENTO = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']

# Parámetros para distribución normal
TEMP_MEDIA = 25.0  # °C
TEMP_DESVIACION = 15.0
TEMP_MIN = 0.0
TEMP_MAX = 110.0

HUMEDAD_MEDIA = 60  # %
HUMEDAD_DESVIACION = 20
HUMEDAD_MIN = 0
HUMEDAD_MAX = 100


def generar_temperatura():
    """
    Genera una temperatura con distribución normal centrada en 25°C
    Rango: [0, 110.00] °C con 2 decimales
    """
    while True:
        temp = random.gauss(TEMP_MEDIA, TEMP_DESVIACION)
        if TEMP_MIN <= temp <= TEMP_MAX:
            return round(temp, 2)


def generar_humedad():
    """
    Genera humedad relativa con distribución normal centrada en 60%
    Rango: [0, 100] % (entero)
    """
    while True:
        humedad = random.gauss(HUMEDAD_MEDIA, HUMEDAD_DESVIACION)
        humedad_int = int(round(humedad))
        if HUMEDAD_MIN <= humedad_int <= HUMEDAD_MAX:
            return humedad_int


def generar_direccion_viento():
    """
    Genera una dirección del viento aleatoria
    Opciones: {N, NO, O, SO, S, SE, E, NE}
    """
    return random.choice(DIRECCIONES_VIENTO)


def generar_data():
    """
    Genera una lectura completa de la estación meteorológica
    Retorna un diccionario con temperatura, humedad y dirección del viento
    """
    data = {
        "temperatura": generar_temperatura(),
        "humedad": generar_humedad(),
        "direccion_viento": generar_direccion_viento(),
    }
    return data


def generar_data_json():
    """
    Genera una lectura y la retorna en formato JSON
    """
    data = generar_data()
    return json.dumps(data, indent=2)


if __name__ == "__main__":
    print("=" * 60)
    print("Simulador de Estación Meteorológica")
    print("=" * 60)
    
    # Generar 5 lecturas de ejemplo
    for i in range(1, 6):
        print(f"\nLectura #{i}:")
        data = generar_data()
        print(f"  Temperatura: {data['temperatura']}°C", f"Humedad: {data['humedad']}%")
        print(f"  Humedad: {data['humedad']}%")
        print(f"  Dirección del viento: {data['direccion_viento']}")
        print(f"\n  JSON: {json.dumps(data, indent=2)}")
