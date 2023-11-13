from confluent_kafka import Producer
import random
import json
import time

# Configuración del productor de Kafka
kafka_config = {
    'bootstrap.servers': '157.245.244.105:9092'  # Servidor Kafka proporcionado
}

# Crear el productor de Kafka
producer = Producer(kafka_config)

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


# Función para simular datos de sensores meteorológicos
def simulate_sensor_data():
    temperature = random.uniform(0, 100)  # Temperatura en grados Celsius
    humidity = random.randint(0, 100)     # Humedad en porcentaje
    wind_directions = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"]
    wind_direction = random.choice(wind_directions)  # Dirección del viento
    return temperature, humidity, wind_direction

# Función para enviar datos de sensores a Kafka
def produce_sensor_data(producer, topic_name):
    temperature, humidity, wind_direction = simulate_sensor_data()
    encoded_data = encode_sensor_data(temperature, humidity, wind_direction)
    producer.produce(topic_name, value=encoded_data)
    print(f"Enviado al topic {topic_name}: Temperatura: {temperature}, Humedad: {humidity}, Dirección del Viento: {wind_direction}")

topic_name = '20262_20009'  # Carnets integrante

# Enviar datos en un intervalo aleatorio de 15 a 30 segundos
while True:
    produce_sensor_data(producer, topic_name)
    producer.flush()
    time.sleep(random.randint(15, 30))  # Intervalo aleatorio entre 15 y 30 segundos
