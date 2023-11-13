from confluent_kafka import Consumer, KafkaException
import json
import matplotlib.pyplot as plt
from collections import deque
import time

# Configuración del consumidor de Kafka
kafka_config = {
    'bootstrap.servers': '157.245.244.105:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}

# Crear el consumidor de Kafka
consumer = Consumer(kafka_config)
topic_name = '20262_20009'  # Carnets de los integrantes
consumer.subscribe([topic_name])

# Colas para almacenar datos de temperatura y humedad
temperatures = deque(maxlen=50)  # Almacena las últimas 50 temperaturas
humidities = deque(maxlen=50)    # Almacena las últimas 50 humedades

def encode_sensor_data(temperature, humidity, wind_direction):
    # Codificar la temperatura
    temp_encoded = int((temperature / 100) * 16383)  # Escalar a 2^14 - 1

    # Codificar la humedad
    hum_encoded = humidity  # Ya es un valor entre 0-100

    # Codificar la dirección del viento
    wind_dir_map = {"N": 0, "NW": 1, "W": 2, "SW": 3, "S": 4, "SE": 5, "E": 6, "NE": 7}
    wind_encoded = wind_dir_map[wind_direction]

    # Combinar los datos codificados en un valor de 24 bits
    combined = (temp_encoded << 10) | (hum_encoded << 3) | wind_encoded

    # Convertir a 3 bytes
    return combined.to_bytes(3, byteorder='big')

def decode_sensor_data(encoded_bytes):
    combined = int.from_bytes(encoded_bytes, byteorder='big')

    # Extraer cada valor
    temperature = ((combined >> 10) & 0x3FFF) / 16383 * 100
    humidity = (combined >> 3) & 0x7F
    wind_direction = combined & 0x07

    # Mapeo inverso para la dirección del viento
    wind_dir_map = {0: "N", 1: "NW", 2: "W", 3: "SW", 4: "S", 5: "SE", 6: "E", 7: "NE"}
    wind_direction = wind_dir_map[wind_direction]

    return temperature, humidity, wind_direction

# Función para procesar mensajes y actualizar gráficos
def consume_and_plot(consumer):
    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6))

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        # Extraer y decodificar datos del mensaje
        encoded_data = msg.value()
        temperature, humidity, wind_direction = decode_sensor_data(encoded_data)
        temperatures.append(temperature)
        humidities.append(humidity)

        # Actualizar gráficos
        ax1.clear()
        ax1.plot(temperatures, label='Temperature')
        ax1.legend()
        ax1.set_ylim(0, 100)

        ax2.clear()
        ax2.plot(humidities, label='Humidity')
        ax2.legend()
        ax2.set_ylim(0, 100)

        plt.pause(0.1)
        plt.draw()

    plt.ioff()
    plt.show()

# Consumir mensajes y graficar
try:
    consume_and_plot(consumer)
except KeyboardInterrupt:
    print("Consumidor interrumpido.")
finally:
    consumer.close()
    print("Consumidor finalizado.")