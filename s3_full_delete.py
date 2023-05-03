#!/usr/bin/env python3
"""
Program:      s3_full_delete.py
Original Author:       https://github.com/vsx-gh
Created:      20170920
Modified:     20230503
Program finds S3 objects with delete markers and deletes all versions
of those objects.
Be very careful running this code!
"""
import os
from collections import defaultdict

import boto3

dry_run = os.environ.get("DRY_RUN") or False

if dry_run:
    print("Dry run enabled. Objects will not be deleted!")

bucket_name = os.environ["BUCKET_NAME"]

if not bucket_name:
    print("No bucket name provided. Exiting...")
    exit(1)

bucket_prefix = os.environ.get("BUCKET_PREFIX") or ""

id_list = defaultdict()  # Holds delete markers
del_obj_list = defaultdict(list)  # Holds all version IDs for objects w/delete markers

profile = os.environ.get("AWS_PROFILE") or os.environ.get("AWS_DEFAULT_PROFILE")
if profile:
    session = boto3.Session(profile_name=profile)
else:
    session = boto3.Session()

s3_client = session.client("s3")
bucket = session.resource("s3").Bucket(bucket_name)
paginator = s3_client.get_paginator("list_object_versions")
del_markers = {}
versions = []

for page in paginator.paginate(Bucket=bucket_name, Prefix=bucket_prefix):
    for marker in page.get("DeleteMarkers", []):
        if marker["IsLatest"]:
            del_markers[marker["Key"]] = marker["VersionId"]

    for version in page.get("Versions", []):
        versions.append(version)

# Get all version IDs for all objects that have eligible delete markers
for item in versions:
    if item["Key"] in del_markers.keys():
        del_obj_list[item["Key"]].append(item["VersionId"])

print("Found {} objects with expired delete markers".format(len(del_obj_list)))

# Remove old versions of object by VersionId
for del_item in del_obj_list:
    if del_item.endswith("/"):
        # Skip folders
        continue

    print("Deleting {}....".format(del_item))

    if dry_run:
        continue

    rm_obj = bucket.Object(del_item)

    for del_id in del_obj_list[del_item]:
        rm_obj.delete(VersionId=del_id)

    # Remove delete marker
    rm_obj.delete(VersionId=del_markers[del_item])

    print("-----\n")
