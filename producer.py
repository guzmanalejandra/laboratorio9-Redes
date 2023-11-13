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

# Función para simular datos de sensores meteorológicos
def simulate_sensor_data():
    temperature = random.uniform(0, 100)  # Temperatura en grados Celsius
    humidity = random.randint(0, 100)     # Humedad en porcentaje
    wind_directions = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"]
    wind_direction = random.choice(wind_directions)  # Dirección del viento
    return {
        "temperature": round(temperature, 2),
        "humidity": humidity,
        "wind_direction": wind_direction
    }

# Función para enviar datos de sensores a Kafka
def produce_sensor_data(producer, topic_name):
    sensor_data = simulate_sensor_data()
    data_str = json.dumps(sensor_data)
    producer.produce(topic_name, value=data_str)
    print(f"Enviado al topic {topic_name}: {data_str}")

topic_name = '20262_20009'  # Carnets integrante

# Enviar datos en un intervalo aleatorio de 15 a 30 segundos
while True:
    produce_sensor_data(producer, topic_name)
    producer.flush()
    time.sleep(random.randint(15, 30))  # Intervalo aleatorio entre 15 y 30 segundos
