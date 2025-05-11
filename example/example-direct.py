#!/usr/bin/env python3
"""Example touch program, invokved with --direct mode.

The input here is fetched directly by invoking aws_dispatcher instead of being
invoked with it as stdin.
"""

import argparse
import sys

import boto3
import aws_dispatcher


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parses an S3 URI like s3://bucket/key into (bucket, key)."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with s3://")
    parts = s3_uri[5:].split("/", 1)
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Format: s3://bucket/key")
    return parts[0], parts[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument('-a', action="store_true")
    parser.add_argument('-b', action="store_true")
    args = parser.parse_args()

    contents = aws_dispatcher.get_array_job_input().decode('utf8')
    print(f"(example) Input: {contents!r}")
    bucket, key = parse_s3_uri(contents)
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=bucket, Key=key)
    print(f"(example) Successfully 'touched' empty file: s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
