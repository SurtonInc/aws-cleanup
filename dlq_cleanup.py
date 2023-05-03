#!/usr/bin/env python
import argparse
import os
import warnings

import boto3

warnings.simplefilter("ignore")


profile = os.environ.get("AWS_PROFILE") or os.environ.get("AWS_DEFAULT_PROFILE")
if profile:
    session = boto3.Session(profile_name=profile)
else:
    session = boto3.Session()

sqs = session.client("sqs")
awslambda = session.client("lambda")


def main(queue_url, purge=False, function_name=None):
    if purge:
        print("Purging Queue...")
        response = sqs.purge_queue(QueueUrl=queue_url)
        print(
            f"Received status code {response['ResponseMetadata']['HTTPStatusCode']} - exiting."
        )
        return

    if not function_name:
        exit("-f, --function-name is required to process messages from the queue.")

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=["SentTimestamp"],
            MessageAttributeNames=["All"],
        )

        if "Messages" not in response:
            print(f"No DLQ messages found in {queue_url}")
            print("Exiting...")
            return

        for message in response["Messages"]:
            print("\n===\n")
            print(
                f"Processing {message['MessageAttributes']['RequestID']['StringValue']}"
            )
            print(
                f"Original Error Message: {message['MessageAttributes']['ErrorMessage']['StringValue']}"
            )
            lambda_response = awslambda.invoke(
                FunctionName=function_name,
                InvocationType="Event",
                Payload=message["Body"],
            )
            if lambda_response["StatusCode"] == 202:
                print("DLQ Lambda Invocation successful. Clearing message from queue.")
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--queue-url", help="SQS Queue URL", required=True)
    parser.add_argument("-p", "--purge", action=argparse.BooleanOptionalAction)
    parser.add_argument("-f", "--function-name", help="Lambda Function Name to Invoke")
    args = parser.parse_args()
    main(args.queue_url, purge=args.purge, function_name=args.function_name)
