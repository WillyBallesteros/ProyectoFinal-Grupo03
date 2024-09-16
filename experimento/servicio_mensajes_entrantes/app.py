import boto3
import time

sqs = boto3.client('sqs', region_name='us-east-1')

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/203918887101/IncomingMessages.fifo'

def process_message(message):
    print(f"Procesando mensaje: {message['Body']}")

def poll_queue():
    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=['All']
        )

        if 'Messages' not in response:
            time.sleep(0.1)
            continue

        for message in response['Messages']:
            process_message(message)

            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )

if __name__ == '__main__':
    print("Iniciando servicio de procesamiento de SQS V1.0.1 ...")
    poll_queue()
