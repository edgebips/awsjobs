#!/usr/bin/env python3
"""Dispatch a subcommand to a specific line in an array file.

This job is intended to be run as a trampoline from a dispatcher job to another
command to be run. If the job is an array job, The indexed content is provided
as stdin to the command.

See inputs in environment variables:

- BATCH_JOB_COMMAND: the command to run.
- BATCH_JOB_ARRAY_FILE: the file containing the array input.
- AWS_BATCH_JOB_ARRAY_INDEX: the index of the array job. This is provided by the
  AWS Batch framework itself.

"""

import argparse
import logging
import os
import sys
import re
import subprocess

import boto3


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parses an S3 URI like s3://bucket/key into (bucket, key)."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with s3://")
    parts = s3_uri[5:].split("/", 1)
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Format: s3://bucket/key")
    return parts[0], parts[1]


def determine_array_input(array_file: str, array_index: int) -> str:
    """Maps the shard to a specific line in the input. Return the line, unparsed."""

    if re.fullmatch("s3://[^/]+/.+", array_file):
        # If the array_file is an S3 URI, we need to download it.
        map_bucket, map_key = parse_s3_uri(array_file)
        s3 = boto3.client("s3")
        resp = s3.get_object(Bucket=map_bucket, Key=map_key)
        data_bytes = resp["Body"].read()
        lines = data_bytes.decode("utf-8").splitlines()
    else:
        # If the array_file is a local file, we can read it directly.
        with open(array_file) as infile:
            lines = infile.read().splitlines()

    if not lines:
        logging.error(f"No lines found in map file {array_file}.")
        sys.exit(1)

    if not (0 <= array_index < len(lines)):
        logging.error(
            f"Error: Array index {array_index} is out of bounds for the number of "
            f"lines ({len(lines)}) in array file {array_file}."
        )
        sys.exit(1)

    return lines[array_index]


def get_array_job_input() -> bytes | None:
    """Fetch the array job's input. If not an array job, return None."""

    # Get the array input from the environment.
    array_index = os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX")
    if array_index is not None:
        array_index = int(array_index)
    array_file = os.environ.get("BATCH_JOB_ARRAY_FILE")
    logging.debug(f"Array file: {array_file}")
    logging.debug(f"Array index: {array_index}")

    # If the job is not an array job, just invoke it without input.
    if array_index is None:
        return None

    # If the job is an array job, we need to determine the input.
    if array_file is None:
        raise ValueError(
            "Missing array file env var for dispatcher: BATCH_JOB_ARRAY_FILE"
        )

    # Determine which S3 object to process.
    array_input = determine_array_input(array_file, array_index)
    logging.info(f"Input: {array_input}")

    return array_input.encode("utf8")


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.strip())
    _ = parser.parse_args()

    # Get the command from the environment.
    command = os.environ.get("BATCH_JOB_COMMAND")
    if command is None:
        raise ValueError("Missing command env var for dispatcher: BATCH_JOB_COMMAND")
    command = os.environ.get("BATCH_JOB_COMMAND")
    logging.info(f"Command: {command}")

    array_input_bytes = get_array_job_input()
    return subprocess.run(command, shell=True, input=array_input_bytes).returncode


if __name__ == "__main__":
    sys.exit(main())
