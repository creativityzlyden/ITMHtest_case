import json
import logging
import pika
import uuid


logging.basicConfig(filename='generator.log', filemode='w',
                    format='время запроса: %(asctime)s.%(msecs)03d - %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
logging.getLogger('answer_app')


def sending():
    # Подключаемся к RabbitMQ
    pika_conn_params = pika.ConnectionParameters(
        host='localhost', port=5672,
        credentials=pika.credentials.PlainCredentials('guest', 'guest'),
    )
    connection = pika.BlockingConnection(pika_conn_params)
    channel = connection.channel()

    # Создаем очередь обработки сообщений
    channel.exchange_declare(exchange='test', exchange_type='direct')

    # Делаем очередь устойчивой
    channel.queue_declare(queue='first_queue', durable=True)

    # Устанавливаем связь между точкой доступа и очередью
    channel.queue_bind(exchange='test', queue="first_queue")

    # Создаем сообщения в цикле
    corr_id = str(uuid.uuid4())

    index = 0
    while True:
        index = index + 1
        if index > 5:
            break
    # Создаем текст сообщение и id
        message = {
            "id": corr_id,
            "first_half": "Hello"
        }

    # Публикуем сообщения
        channel.basic_publish(
            exchange='test',
            routing_key='first_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                correlation_id=corr_id,
                delivery_mode=2,
            ))

        print(f" [x] Sent message id {message['id']} text: {message['first_half']}")
        logging.warning(f'индификатор запроса № {message["id"]} {message["first_half"]}')

    connection.close()


sending()
