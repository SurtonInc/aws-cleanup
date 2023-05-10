#!/usr/bin/env python
import argparse
import os

import boto3

profile = os.environ.get("AWS_PROFILE") or os.environ.get("AWS_DEFAULT_PROFILE")
if profile:
    session = boto3.Session(profile_name=profile)
else:
    session = boto3.Session()


def main(bucket_name):
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    bucket.object_versions.delete()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Completely empties versioned S3 bucket"
    )
    parser.add_argument("bucket", type=str, help="name of S3 bucket")
    args = parser.parse_args()
    main(args.bucket)
