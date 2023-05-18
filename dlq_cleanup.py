#!/usr/bin/env python
import argparse
import os
import warnings

import boto3

warnings.simplefilter("ignore")


profile = os.environ.get("AWS_PROFILE") or os.environ.get("AWS_DEFAULT_PROFILE")
region = (
    os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
)
if profile:
    session = boto3.Session(profile_name=profile, region_name=region)
else:
    session = boto3.Session(region_name=region)

sqs = session.client("sqs")
awslambda = session.client("lambda")


def main(queue_url, purge=False, redrive_queue_url=None, function_name=None):
    if purge:
        print("Purging Queue...")
        response = sqs.purge_queue(QueueUrl=queue_url)
        print(
            f"Received status code {response['ResponseMetadata']['HTTPStatusCode']} - exiting."
        )
        return

    if not redrive_queue_url and not function_name:
        exit(
            "-r, --redrive-queue-url or -f, --function-name is required to process messages from the queue."
        )

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["SentTimestamp"],
            MessageAttributeNames=["All"],
        )

        if "Messages" not in response or len(response["Messages"]) == 0:
            print(f"No DLQ messages found in {queue_url}")
            print("Exiting...")
            return

        for message in response["Messages"]:
            print("\n===\n")
            print(f"Processing {message}")

            if "Body" not in message or "ReceiptHandle" not in message:
                print("Message is missing Body or ReceiptHandle - unable to purge")
                continue

            if redrive_queue_url:
                print("Redriving message to queue")
                sqs.send_message(
                    QueueUrl=redrive_queue_url,
                    MessageBody=message["Body"],
                    MessageAttributes=message.get("MessageAttributes", {}),
                )
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                )
                continue
            elif function_name:
                lambda_response = awslambda.invoke(
                    FunctionName=function_name,
                    InvocationType="Event",
                    Payload=message["Body"],
                )
                if lambda_response["StatusCode"] == 202:
                    print(
                        "DLQ Lambda Invocation successful. Clearing message from queue."
                    )
                    sqs.delete_message(
                        QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                    )
            else:
                exit("No redrive queue or function name specified - exiting.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--queue-url", help="SQS Queue URL", required=True)
    parser.add_argument("-p", "--purge", action=argparse.BooleanOptionalAction)
    parser.add_argument("-r", "--redrive-queue-url", help="SQS Queue URL to Redrive")
    parser.add_argument("-f", "--function-name", help="Lambda Function Name to Invoke")
    args = parser.parse_args()
    main(
        args.queue_url,
        purge=args.purge,
        redrive_queue_url=args.redrive_queue_url,
        function_name=args.function_name,
    )
