import pika
import time
import json
import random


def callback(ch, method, properties, body):

    # Делаем случайную паузу от 0 до 1 сек
    random.seed()
    n = random.random()
    time.sleep(n)

    # Сообщение
    message = json.loads(body)

    # Формируем ответ
    message = message["first_half"] + " World"

    print(f" [x] Result: {message}, time: {n}")

    # Подтверждаем получение RabbitMQ
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    # Подключаемся к RabbitMQ
    pika_conn_params = pika.ConnectionParameters(
        host='localhost', port=5672,
        credentials=pika.credentials.PlainCredentials('guest', 'guest'),
    )
    connection = pika.BlockingConnection(pika_conn_params)
    channel = connection.channel()

    channel.queue_declare(queue='first_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    # Подтверждение получения и очередь задач
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='first_queue', on_message_callback=callback)

    channel.start_consuming()


main()
