import boto3
import time
import json
import os
from datetime import datetime

sqs = boto3.client('sqs', region_name='us-east-1')
sns = boto3.client('sns', region_name='us-east-1')

QUEUE_URL = os.getenv('SQS_QUEUE_URL')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
MAX_NUMBER_OF_MESSAGES = int(os.getenv('MAX_NUMBER_OF_MESSAGES', '1'))
WAIT_QUEUE_TIME_SECONDS = int(os.getenv('WAIT_QUEUE_TIME_SECONDS', '20'))
WAIT_TIME_EMPTY_QUEUE_REINTENT = int(os.getenv('WAIT_TIME_EMPTY_QUEUE_REINTENT', '100'))

if MAX_NUMBER_OF_MESSAGES < 1:
    MAX_NUMBER_OF_MESSAGES = 1

if WAIT_QUEUE_TIME_SECONDS < 0:
    WAIT_QUEUE_TIME_SECONDS = 20

if WAIT_TIME_EMPTY_QUEUE_REINTENT <= 0:
    WAIT_TIME_EMPTY_QUEUE_REINTENT = 100

def process_message(message):
    timestamp_received = datetime.now().isoformat()
    print(f"Procesando mensaje: {message['Body']}")
    body = json.loads(message['Body'])
    tiempo_estimado = int(body.get('tiempoEstimadoProcesamiento', 500)) / 1000
    
    message_deduplication_id = message.get('Attributes', {}).get('MessageDeduplicationId', 'default-deduplication-id')
    message_group_id = message.get('Attributes', {}).get('MessageGroupId', 'default-group-id')
    first_receive_timestamp_ms = int(message.get('Attributes', {}).get('ApproximateFirstReceiveTimestamp'))
    current_timestamp_ms = int(datetime.now().timestamp() * 1000)
    message_age = current_timestamp_ms - first_receive_timestamp_ms

    message_id = message.get('MessageId', 'default-message-id')
    
    print(f"Procesando mensaje: {message_id} con tiempo estimado de procesamiento: {tiempo_estimado} segundos")

    time.sleep(tiempo_estimado)
    timestamp_processed = datetime.now().isoformat()

    body['timestamp_received'] = timestamp_received
    body['timestamp_processed'] = timestamp_processed
    body['message_deduplication_id'] = message_deduplication_id
    body['message_group_id'] = message_group_id
    body['message_age'] = message_age
    body['message_id'] = message_id

    notify_sns(body)

def notify_sns(body):
    mensaje_sns = json.dumps(body)
    
    response = sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=mensaje_sns,
        Subject="Notificación de procesamiento de mensaje"
    )

    print(f"Notificación enviada a SNS: {response['MessageId']} del mensaje: {mensaje_sns}")

def poll_queue():
    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
            WaitTimeSeconds=WAIT_QUEUE_TIME_SECONDS,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
             MessageSystemAttributeNames=['MessageDeduplicationId', 'MessageGroupId']
        )

        if 'Messages' not in response:
            time.sleep(WAIT_TIME_EMPTY_QUEUE_REINTENT/1000)
            continue

        for message in response['Messages']:
            process_message(message)

            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )

if __name__ == '__main__':
    print("Iniciando servicio de procesamiento de SQS V1.0.4 ...")
    poll_queue()
