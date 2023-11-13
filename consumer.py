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

# Función para procesar mensajes y actualizar gráficos
def consume_and_plot(consumer):
    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6))

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        # Extraer datos del mensaje
        data = json.loads(msg.value().decode('utf-8'))
        temperatures.append(data['temperature'])
        humidities.append(data['humidity'])

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
