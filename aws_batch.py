#!/usr/bin/env python3
"""Run a job defined in a Docker container with AWS Batch on AWS Fargate Spot instances.

This script allows you to run a single mapper stage where the input is defined
by a file pointing to the parameters for the mapper. The script has three subcommands:

- setup: Creates a compute environment and job queue with Fargate Spot instancess.
- teardown: Deleted the CE and JQ.
- submit: Given a container, submit a job for that container. If a Python program is
  provided instead, it can build a docker container inline for it.

"""

import argparse
import datetime
import fnmatch
import glob
import json
import logging
import os
import pprint
import re
import tempfile
import time

from botocore.client import BaseClient
import boto3
import jsonschema


CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "ResourcePrefix": {"type": "string"},
        "AccountId": {"type": "string", "pattern": "^[0-9]{12}$"},
        "Region": {"type": "string"},
        "Vpc": {"type": "string", "pattern": "^vpc-[0-9a-fA-F]{17}$"},
        "Subnets": {
            "type": "array",
            "items": {"type": "string", "pattern": "^subnet-[0-9a-fA-F]{17}$"},
            "minItems": 1,
        },
        "SecurityGroup": {"type": "string", "pattern": "^sg-[0-9a-fA-F]{17}$"},
        "RoleBatch": {"type": "string", "pattern": "^arn:aws:iam::[0-9]{12}:role/.+$"},
        "RoleExec": {"type": "string", "pattern": "^arn:aws:iam::[0-9]{12}:role/.+$"},
        "EcrRepository": {
            "type": "string",
            "pattern": "^[0-9]{12}\\.dkr\\.ecr\\.[a-z0-9-]+\\.amazonaws\\.com$",
        },
        "JobVcpu": {"type": "integer", "minimum": 1},
        "JobMemoryMiB": {
            "type": "integer",
            "minimum": 32,
        },  # Fargate minimum is 512MB for 0.25vCPU, but higher for more vCPU. General min is 32.
        "RunBucket": {"type": "string"},
        "RunPrefix": {"type": "string"},
    },
    "required": [
        "ResourcePrefix",
        "AccountId",
        "Region",
        "Vpc",
        "Subnets",
        "SecurityGroup",
        "RoleBatch",
        "RoleExec",
        "EcrRepository",
        "JobVcpu",
        "JobMemoryMiB",
        "RunBucket",
        "RunPrefix",
    ],
    "additionalProperties": False,
}


def create_compute_environment(
    batch_client: BaseClient, compute_env: str, config: dict
):
    """Creates an AWS Batch Fargate compute environment."""

    try:
        response = batch_client.create_compute_environment(
            computeEnvironmentName=compute_env,
            type="MANAGED",
            state="ENABLED",
            computeResources={
                "type": "FARGATE_SPOT",  # Or FARGATE for on-demand
                "maxvCpus": 256,  # Adjust as needed
                "subnets": config["Subnets"],
                "securityGroupIds": [config["SecurityGroup"]],
            },
            serviceRole=config["RoleBatch"],  # Use the Batch service role ARN
        )
        logging.info(f"Compute environment '{compute_env}' creation initiated.")
        logging.info(f"Response: {response}")
        return response

    except batch_client.exceptions.ClientException as exc:
        logging.error(f"Failed to create compute environment '{compute_env}': {exc}")
        raise


def wait_for_compute_environment_ready(batch_client: BaseClient, compute_env: str):
    """Waits until the specified compute environment is VALID and ENABLED."""
    logging.info(f"Waiting for compute environment '{compute_env}' to become ready...")
    while True:
        try:
            response = batch_client.describe_compute_environments(
                computeEnvironments=[compute_env]
            )
            if not response.get("computeEnvironments"):
                logging.error(
                    f"Compute environment '{compute_env}' not found while waiting."
                )
                # Or raise an exception, depending on desired behavior
                raise ValueError(f"Compute environment '{compute_env}' disappeared.")

            env = response["computeEnvironments"][0]
            env_status = env.get("status", "UNKNOWN")
            env_state = env.get("state", "UNKNOWN")

            logging.info(
                f"Compute environment '{compute_env}' status: {env_status}, state: {env_state}"
            )

            if env_state == "ENABLED" and env_status == "VALID":
                logging.info(f"Compute environment '{compute_env}' is ready.")
                break
            elif env_status in ["CREATING", "UPDATING"]:
                logging.info(
                    "Compute environment is still being created/updated. Waiting..."
                )
            elif env_status == "INVALID":
                logging.error(
                    f"Compute environment '{compute_env}' entered INVALID status: {env.get('statusReason', 'No reason provided')}"
                )
                raise ValueError(f"Compute environment '{compute_env}' became INVALID.")
            elif env_status == "DELETING":
                logging.error(
                    f"Compute environment '{compute_env}' is DELETING unexpectedly."
                )
                raise ValueError(
                    f"Compute environment '{compute_env}' is being deleted."
                )
            else:
                logging.warning(
                    f"Compute environment '{compute_env}' in unexpected status '{env_status}' or state '{env_state}'. Waiting..."
                )

        except batch_client.exceptions.ClientException as exc:
            logging.error(
                f"An error occurred while checking compute environment status: {exc}"
            )
            raise

        time.sleep(2)  # Wait before checking again


def delete_compute_environment(batch_client: BaseClient, compute_env: str):
    """Disables and deletes an AWS Batch compute environment."""
    try:
        # 1. Disable the compute environment
        logging.info(f"Attempting to disable compute environment '{compute_env}'...")
        batch_client.update_compute_environment(
            computeEnvironment=compute_env, state="DISABLED"
        )
        logging.info(f"Compute environment '{compute_env}' disabling initiated.")

        # 2. Wait for the environment to become DISABLED (or INVALID)
        # A more robust solution would use waiters or check status periodically.
        # This is a simple wait loop for demonstration.
        logging.info("Waiting for compute environment to become disabled...")
        while True:
            response = batch_client.describe_compute_environments(
                computeEnvironments=[compute_env]
            )
            if not response.get("computeEnvironments"):
                logging.warning(
                    f"Compute environment '{compute_env}' not found during status check."
                )
                # Might have been deleted already or never existed.
                return

            env_status = response["computeEnvironments"][0]["status"]
            env_state = response["computeEnvironments"][0]["state"]

            logging.info(f"Current status: {env_status}, state: {env_state}")

            if env_state == "DISABLED":
                if env_status == "VALID":
                    logging.info(f"Compute environment '{compute_env}' is disabled.")
                    break
                elif env_status == "INVALID":
                    logging.warning(
                        f"Compute environment '{compute_env}' entered INVALID state while disabling. Proceeding with deletion attempt."
                    )
                    break

            elif env_state == "ENABLED" and env_status == "UPDATING":
                logging.info("Compute environment is still updating (disabling)...")

            elif env_status == "DELETING":
                logging.warning(
                    f"Compute environment '{compute_env}' is already being deleted."
                )
                return  # Nothing more to do

            else:
                logging.warning(
                    f"Unexpected status ({env_status}) or state ({env_state}) while waiting for disable. Will attempt deletion anyway after delay."
                )
                # Add a small delay before trying delete in unexpected states
                time.sleep(2)
                break  # Proceed to delete attempt

            time.sleep(2)

        # 3. Delete the compute environment
        logging.info(f"Attempting to delete compute environment '{compute_env}'...")
        response = batch_client.delete_compute_environment(
            computeEnvironment=compute_env
        )
        logging.info(f"Compute environment '{compute_env}' deletion initiated.")
        logging.info(f"Response: {response}")

    except batch_client.exceptions.ClientException as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "ComputeEnvironmentNotFoundException":
            logging.warning(
                f"Compute environment '{compute_env}' not found. It might already be deleted."
            )
        else:
            logging.error(
                f"Failed to delete compute environment '{compute_env}': {exc}"
            )
            raise


def create_job_queue(
    batch_client: BaseClient,
    job_queue_name: str,
    compute_env: str,
    priority: int = 1,
):
    """Creates an AWS Batch job queue."""
    try:
        response = batch_client.create_job_queue(
            jobQueueName=job_queue_name,
            state="ENABLED",
            priority=priority,  # Lower number means higher priority
            computeEnvironmentOrder=[
                {"order": 1, "computeEnvironment": compute_env},
            ],
            # tags={} # Optional: Add tags if needed
        )
        logging.info(f"Job queue '{job_queue_name}' creation initiated.")
        logging.info(f"Response: {response}")
        return response
    except batch_client.exceptions.ClientException as exc:
        logging.error(f"Failed to create job queue '{job_queue_name}': {exc}")
        raise


def wait_for_job_queue_ready(batch_client: BaseClient, job_queue_name: str):
    """Waits until the specified job queue is VALID and ENABLED."""
    logging.info(f"Waiting for job queue '{job_queue_name}' to become ready...")
    while True:
        try:
            response = batch_client.describe_job_queues(jobQueues=[job_queue_name])
            if not response.get("jobQueues"):
                logging.error(f"Job queue '{job_queue_name}' not found while waiting.")
                raise ValueError(f"Job queue '{job_queue_name}' disappeared.")

            queue = response["jobQueues"][0]
            queue_status = queue.get("status", "UNKNOWN")
            queue_state = queue.get("state", "UNKNOWN")

            logging.info(
                f"Job queue '{job_queue_name}' status: {queue_status}, state: {queue_state}"
            )

            if queue_state == "ENABLED" and queue_status == "VALID":
                logging.info(f"Job queue '{job_queue_name}' is ready.")
                break
            elif queue_status in ["CREATING", "UPDATING"]:
                logging.info("Job queue is still being created/updated. Waiting...")
            elif queue_status == "INVALID":
                logging.error(
                    f"Job queue '{job_queue_name}' entered INVALID status: {queue.get('statusReason', 'No reason provided')}"
                )
                raise ValueError(f"Job queue '{job_queue_name}' became INVALID.")
            elif queue_status == "DELETING":
                logging.error(f"Job queue '{job_queue_name}' is DELETING unexpectedly.")
                raise ValueError(f"Job queue '{job_queue_name}' is being deleted.")
            else:
                logging.warning(
                    f"Job queue '{job_queue_name}' in unexpected status '{queue_status}' or state '{queue_state}'. Waiting..."
                )

        except batch_client.exceptions.ClientException as exc:
            logging.error(f"An error occurred while checking job queue status: {exc}")
            raise

        time.sleep(2)


def delete_job_queue(batch_client: BaseClient, job_queue_name: str):
    """Disables and deletes an AWS Batch job queue."""
    try:
        # 1. Disable the job queue
        logging.info(f"Attempting to disable job queue '{job_queue_name}'...")
        batch_client.update_job_queue(jobQueue=job_queue_name, state="DISABLED")
        logging.info(f"Job queue '{job_queue_name}' disabling initiated.")

        # 2. Wait for the queue to become DISABLED
        logging.info("Waiting for job queue to become disabled...")
        while True:
            response = batch_client.describe_job_queues(jobQueues=[job_queue_name])
            if not response.get("jobQueues"):
                logging.warning(
                    f"Job queue '{job_queue_name}' not found during status check."
                )
                return  # Might have been deleted already

            queue_status = response["jobQueues"][0]["status"]
            queue_state = response["jobQueues"][0]["state"]

            logging.info(f"Current status: {queue_status}, state: {queue_state}")

            if queue_state == "DISABLED":
                if queue_status == "VALID":
                    logging.info(f"Job queue '{job_queue_name}' is disabled.")
                    break
                elif queue_status == "UPDATING":
                    logging.info("Job queue is still updating (disabling)...")
                else:  # e.g., INVALID
                    logging.warning(
                        f"Job queue '{job_queue_name}' entered status '{queue_status}' while disabling. Proceeding with deletion attempt."
                    )
                    break

            elif queue_status == "DELETING":
                logging.warning(
                    f"Job queue '{job_queue_name}' is already being deleted."
                )
                return

            else:
                logging.warning(
                    f"Unexpected status ({queue_status}) or state ({queue_state}) while waiting for disable. Will attempt deletion anyway after delay."
                )
                time.sleep(2)
                break  # Proceed to delete attempt

            time.sleep(2)

        # 3. Delete the job queue
        logging.info(f"Attempting to delete job queue '{job_queue_name}'...")
        response = batch_client.delete_job_queue(jobQueue=job_queue_name)
        logging.info(f"Job queue '{job_queue_name}' deletion initiated.")
        logging.info(f"Response: {response}")

    except batch_client.exceptions.ClientException as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "JobQueueNotFoundException":
            logging.warning(
                f"Job queue '{job_queue_name}' not found. It might already be deleted."
            )
        else:
            logging.error(f"Failed to delete job queue '{job_queue_name}': {exc}")
            raise


def wait_for_job_queue_deletion(batch_client: BaseClient, job_queue_name: str):
    """Waits until the specified job queue is deleted."""
    logging.info(f"Waiting for job queue '{job_queue_name}' to be deleted...")
    while True:
        try:
            response = batch_client.describe_job_queues(jobQueues=[job_queue_name])
            if not response.get("jobQueues"):
                logging.info(f"Job queue '{job_queue_name}' successfully deleted.")
                break  # Queue is gone
            else:
                # Check status just in case (optional, but informative)
                queue_status = response["jobQueues"][0].get("status", "UNKNOWN")
                logging.info(
                    f"Job queue '{job_queue_name}' still exists with status '{queue_status}'. Waiting..."
                )

        except batch_client.exceptions.ClientException as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code == "JobQueueNotFoundException":
                logging.info(
                    f"Job queue '{job_queue_name}' successfully deleted (confirmed by NotFoundException)."
                )
                break  # Queue is gone
            else:
                logging.error(
                    f"An error occurred while checking job queue status: {exc}"
                )
                raise

        time.sleep(2)


def wait_for_job_definition_ready(batch_client: BaseClient, job_def_name: str):
    """Waits until the specified job definition is ACTIVE."""
    logging.info(f"Waiting for job definition '{job_def_name}' to become ACTIVE...")
    while True:
        try:
            # Note: DescribeJobDefinitions can return multiple revisions.
            # We usually care about the latest one being active.
            # For simplicity, we check if *any* active definition with that name exists.
            # A more robust check might involve getting the specific revision from
            # register_job_definition and checking only that one.
            response = batch_client.describe_job_definitions(
                jobDefinitionName=job_def_name,
                status="ACTIVE",  # Filter for active ones directly
            )

            if response.get("jobDefinitions") and response["jobDefinitions"]:
                # Check if the latest revision (usually the first in the list returned by default)
                # matches the name and is active.
                # AWS sorts by revision descending by default.
                latest_def = response["jobDefinitions"][0]
                if (
                    latest_def.get("jobDefinitionName") == job_def_name
                    and latest_def.get("status") == "ACTIVE"
                ):
                    logging.info(
                        f"Job definition '{job_def_name}' (revision {latest_def.get('revision')}) is ACTIVE."
                    )
                    break
                else:
                    # This case should be rare if filtering by ACTIVE works as expected
                    logging.info(
                        f"Found definitions for '{job_def_name}', but latest is not ACTIVE or name mismatch. Waiting..."
                    )

            else:
                # Check if it exists but is inactive
                try:
                    all_defs_resp = batch_client.describe_job_definitions(
                        jobDefinitionName=job_def_name
                    )
                    if all_defs_resp.get("jobDefinitions"):
                        latest_status = all_defs_resp["jobDefinitions"][0].get(
                            "status", "UNKNOWN"
                        )
                        logging.info(
                            f"Job definition '{job_def_name}' found, but status is '{latest_status}'. Waiting..."
                        )
                    else:
                        # Should not happen if create was called, maybe a race condition or deletion
                        logging.warning(
                            f"Job definition '{job_def_name}' not found yet. Waiting..."
                        )
                except batch_client.exceptions.ClientException as inner_e:
                    # Handle case where describe fails entirely (e.g., definition truly doesn't exist)
                    logging.warning(
                        f"Could not describe job definition '{job_def_name}' to check status: {inner_e}. Waiting..."
                    )

        except batch_client.exceptions.ClientException as exc:
            logging.error(
                f"An error occurred while checking job definition status: {exc}"
            )
            raise

        time.sleep(2)


def create_job_definition(
    batch_client: BaseClient,
    job_def_name: str,
    image_uri: str,
    execution_role_arn: str,
    vcpu: int,
    memory_mib: int,
):
    """Creates an AWS Batch Fargate job definition."""

    # - Job/Task Role: grants permissions to your application code
    #   running inside the container.
    # - Task Execution Role: grants permissions to the ECS agent
    #   managing the container lifecycle.
    #
    # The standard practice is to create a dedicated IAM role (often
    # named ecsTaskExecutionRole) with the
    # AmazonECSTaskExecutionRolePolicy attached and use that ARN as
    # the Task Execution Role in your Job Definition.

    try:
        response = batch_client.register_job_definition(
            jobDefinitionName=job_def_name,
            type="container",
            platformCapabilities=["FARGATE"],  # Specify Fargate capability
            containerProperties={
                "image": image_uri,
                # "command": [
                #     "python3",
                #     "bin/process_day.py",
                #     "--s3-map-file-uri=s3://speculative/run/input_mapping",
                # ],
                "jobRoleArn": execution_role_arn,  # Optional: Role for the job container itself if it needs AWS permissions
                "executionRoleArn": execution_role_arn,  # Required for Fargate
                "resourceRequirements": [
                    {"type": "VCPU", "value": str(vcpu)},
                    {"type": "MEMORY", "value": str(memory_mib)},
                ],
                "ephemeralStorage": {
                    "sizeInGiB": 40,
                },
                "fargatePlatformConfiguration": {  # Optional: Specify Fargate platform version
                    "platformVersion": "LATEST"
                },
                "runtimePlatform": {
                    "cpuArchitecture": "X86_64",
                    "operatingSystemFamily": "LINUX",
                },
                "networkConfiguration": {  # Optional: Assign public IP; this is necessary for things to work.
                    "assignPublicIp": "ENABLED"
                },
                # 'environment': [{'name': 'VAR_NAME', 'value': 'VAR_VALUE'}], # Optional environment variables
                # 'logConfiguration': {}, # Optional: Customize log driver (defaults to awslogs)
            },
            # tags={}, # Optional: Add tags if needed
            # retryStrategy={"evaluateOnExit": []}, # Optional: Define retry strategy
            # timeout={}, # Optional: Define job timeout
            # propagateTags=False, # Optional: Propagate tags to ECS tasks
            # schedulingPriority=0 # Optional: Set scheduling priority (0-9999)
        )
        logging.info(f"Job definition '{job_def_name}' registration initiated.")
        logging.info(f"Response: {response}")
        # Note: Job definitions are versioned. This returns the name and revision.
        return response["jobDefinitionArn"], response["revision"]

    except batch_client.exceptions.ClientException as exc:
        logging.error(f"Failed to register job definition '{job_def_name}': {exc}")
        raise


def delete_job_definition(batch_client: BaseClient, job_def_name: str):
    """Deregisters all revisions of an AWS Batch job definition."""
    logging.info(
        f"Attempting to deregister all revisions of job definition '{job_def_name}'..."
    )
    try:
        # List all revisions of the job definition
        paginator = batch_client.get_paginator("describe_job_definitions")
        page_iterator = paginator.paginate(jobDefinitionName=job_def_name)

        revisions_to_deregister = []
        for page in page_iterator:
            for job_def in page.get("jobDefinitions", []):
                # Ensure we only try to deregister the correct definition name
                if job_def.get("jobDefinitionName") == job_def_name:
                    revisions_to_deregister.append(
                        job_def["jobDefinitionArn"]
                    )  # Use ARN which includes revision

        if not revisions_to_deregister:
            logging.warning(
                "No active or inactive revisions found for "
                f"job definition '{job_def_name}'. It might already be deleted."
            )
            return

        logging.info(f"Found {len(revisions_to_deregister)} revision(s) to deregister.")

        for revision_arn in revisions_to_deregister:
            try:
                logging.info(f"Deregistering revision: {revision_arn}")
                batch_client.deregister_job_definition(jobDefinition=revision_arn)
                logging.info(f"Successfully deregistered revision: {revision_arn}")
                # Add a small delay to avoid potential throttling if many revisions exist
                time.sleep(1)
            except batch_client.exceptions.ClientException as inner_e:
                error_code = inner_e.response.get("Error", {}).get("Code")
                if error_code == "JobDefinitionNotFoundException":
                    logging.warning(
                        f"Revision '{revision_arn}' not found. "
                        "It might have been deleted already."
                    )
                else:
                    logging.error(
                        f"Failed to deregister revision '{revision_arn}': {inner_e}"
                    )
                    # Decide if you want to continue deregistering others or raise
                    # raise # Option: Stop on first error

        logging.info(
            f"Finished attempting to deregister job definition '{job_def_name}'."
        )

    except batch_client.exceptions.ClientException as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "JobDefinitionNotFoundException":
            logging.warning(
                f"Job definition '{job_def_name}' not found when listing revisions. "
                "It might already be deleted."
            )
        else:
            logging.error(
                f"Failed to list or deregister job definition '{job_def_name}': {exc}"
            )
            raise


def submit_job(
    batch_client: BaseClient,
    job_name: str,
    job_definition: str,
    job_queue: str,
    size: int,
    s3_map_uri: str,
):
    """Submits a job to AWS Batch."""

    try:
        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            arrayProperties={"size": size},
            containerOverrides={
                "command": ["python3", "aws_dispatcher.py"],
                "environment": [
                    {
                        "name": "BATCH_JOB_ARRAY_FILE",
                        "value": s3_map_uri,
                    },
                ],
            },
        )
        logging.info(f"Job '{job_name}' submitted successfully.")
        logging.info(f"Job ID: {response['jobId']}")
        return response
    except batch_client.exceptions.ClientException as exc:
        logging.error(f"Failed to submit job '{job_name}': {exc}")
        raise


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parses an S3 URI like s3://bucket/key into (bucket, key)."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with s3://")
    parts = s3_uri[5:].split("/", 1)
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Format: s3://bucket/key")
    return parts[0], parts[1]


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parses an S3 URI like s3://bucket/key into (bucket, key)."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with s3://")
    parts = s3_uri[5:].split("/", 1)
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Format: s3://bucket/key")
    return parts[0], parts[1]


def list_s3_files_with_glob(s3_pattern: str) -> list[str]:
    """Lists files in an S3 bucket based on a key pattern that may include a prefix
    and a globbing pattern for the basename. It automatically splits the prefix
    from the basename pattern.
    """

    if not s3_pattern.startswith("s3://"):
        raise ValueError("Invalid pattern.")
    bucket, key_pattern = parse_s3_uri(s3_pattern)

    prefix = ''
    basename_pattern = key_pattern
    if '/' in key_pattern:
        prefix, basename_pattern = key_pattern.rsplit('/', 1)
        prefix += '/'  # Ensure the prefix ends with a '/' for S3

    if any(char in prefix for char in ['*', '?', '[']):
        raise ValueError("Globbing patterns are only allowed in the basename part of the key pattern.")

    s3 = boto3.client('s3')
    matching_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                basename = os.path.basename(key)
                if fnmatch.fnmatch(basename, basename_pattern):
                    suffix_after = key[len(prefix + basename):]
                    matching_keys.append(key)
    return [f"s3://{bucket}/{key}" for key in matching_keys]


def prepare_mapping_file(
    job_name: str, input_mapping_file: str, config: dict
) -> tuple[str, int]:
    """Copies the input mapping file on S3 for the workers to use and return the
    number of jobs to run."""

    # If the file contains a pattern, interpreting it as a pattern will itself
    # will be the input contents.
    if "*" in input_mapping_file:
        if re.match("s3://", input_mapping_file):
            inputs = list_s3_files_with_glob(input_mapping_file)
        else:
            # Note that it's questionable how useful that will be.
            inputs = glob.glob(input_mapping_file)
        tmpfile = tempfile.NamedTemporaryFile(mode='w')
        for input_ in inputs:
            print(input_, file=tmpfile)
        tmpfile.flush()
        input_mapping_file = tmpfile.name

    s3 = boto3.client("s3")
    if re.fullmatch("s3://[^/]+/.+", input_mapping_file):
        # The file is already on s3; no need to copy it.
        s3_map_uri = input_mapping_file

        # Get the number of jobs from the s3 object.
        bucket, key = parse_s3_uri(s3_map_uri)
        response = s3.get_object(Bucket=bucket, Key=key)
        contents = response["Body"].read().decode("utf-8")
        num_jobs = len(contents.splitlines())

    else:
        # Copy the input mapping to a file on s3.
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_key = f"{config['RunPrefix']}/{job_name}.{timestamp_str}"
        s3.upload_file(input_mapping_file, config["RunBucket"], run_key)
        s3_map_uri = f"s3://{config['RunBucket']}/{run_key}"

        # Get the number of jobs from the local file.
        with open(input_mapping_file) as infile:
            num_jobs = len(infile.readlines())

    return s3_map_uri, num_jobs


def get_ecr_registry(region: str) -> str:
    """Return the ECR registry URI for the given region."""
    ecr_client = boto3.client("ecr", region_name=region)
    auth_data = ecr_client.get_authorization_token()
    authorization_token = auth_data["authorizationData"][0]["authorizationToken"]
    https_registry = auth_data["authorizationData"][0]["proxyEndpoint"]
    return https_registry.replace("https://", "")


def read_and_validate_config(config_filename: str) -> dict:
    with open(config_filename) as config_file:
        content = config_file.read()
        content = re.sub(
            r"//.*?$|/\*.*?\*/", "", content, flags=re.MULTILINE | re.DOTALL
        )
        config = json.loads(content)
        try:
            jsonschema.validate(instance=config, schema=CONFIG_SCHEMA)
            logging.info("Configuration file is valid.")
        except jsonschema.exceptions.ValidationError as err:
            logging.error(f"Configuration file validation error: {err.message}")
            # Optionally, re-raise or exit if config is invalid
            raise SystemExit(f"Invalid configuration: {err.message}")
    return config


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)-8s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument(
        "-c",
        "--config",
        action="store",
        default=os.environ.get("AWS_BATCH_CONFIG"),
        help="Configuration JSON file (see example JSON for schema).",
    )

    subparsers = parser.add_subparsers(dest="command", help="Sub-command help")

    parser_setup = subparsers.add_parser(
        "setup", help="Create the AWS Batch compute environment and job queue."
    )

    parser_teardown = subparsers.add_parser(
        "teardown", help="Delete the AWS Batch job queue and compute environment."
    )

    parser_submit = subparsers.add_parser("submit", help="Submit a job to the queue.")
    parser_submit.add_argument(
        "--job-name",
        help="Allow overriding a name for the job; default is to use the container label.",
    )
    parser_submit.add_argument(
        "image_uri",
        action="store",
        help="Container image URI",
    )
    parser_submit.add_argument(
        "mapping_file",
        help=(
            "A filename containing input lines to be mapped to. "
            "Can be a file on s3 or otherwise."
        ),
    )

    args = parser.parse_args()

    # Read the JSON configuration.
    if not args.config:
        parser.error("Missing --config; argument is required, or set AWS_BATCH_CONFIG.")
    config = read_and_validate_config(args.config)
    batch_client = boto3.client("batch")

    # Define resource names based on config prefix
    resource_prefix = config["ResourcePrefix"]
    compute_env_name = f"{resource_prefix}-compute"
    job_queue_name = f"{resource_prefix}-queue"
    job_definition_name = f"{resource_prefix}-job"

    if args.command == "setup":
        logging.info("Setting up AWS Batch resources...")

        create_compute_environment(batch_client, compute_env_name, config)
        wait_for_compute_environment_ready(batch_client, compute_env_name)

        create_job_queue(batch_client, job_queue_name, compute_env_name)
        wait_for_job_queue_ready(
            batch_client, job_queue_name
        )  # Wait for queue to be ready

        logging.info("Setup complete.")

    elif args.command == "teardown":
        logging.info("Tearing down AWS Batch resources...")

        delete_job_definition(batch_client, job_definition_name)

        delete_job_queue(batch_client, job_queue_name)
        wait_for_job_queue_deletion(batch_client, job_queue_name)

        delete_compute_environment(batch_client, compute_env_name)

        logging.info("Teardown complete.")

    elif args.command == "submit":
        job_name = args.job_name or args.image_uri.split(":")[0]
        logging.info(f"Submitting job '{job_name}'...")

        s3_map_uri, num_jobs = prepare_mapping_file(job_name, args.mapping_file, config)

        ecr_repository = get_ecr_registry(config["Region"])
        image_uri = f"{ecr_repository}/{args.image_uri}"

        logging.info(f"Uploaded mapping input to '{s3_map_uri}'")
        logging.info(f"Using image '{image_uri}'")
        logging.info(f"Launching {num_jobs} jobs.")

        create_job_definition(
            batch_client,
            job_definition_name,
            image_uri,
            config["RoleExec"],
            config["JobVcpu"],
            config["JobMemoryMiB"],
        )
        # Wait for the job definition to become active
        wait_for_job_definition_ready(batch_client, job_definition_name)

        # Submit the job.
        response = submit_job(
            batch_client,
            job_name,
            job_definition_name,
            job_queue_name,
            num_jobs,
            s3_map_uri,
        )
        job_url = (
            f"https://{config['Region']}.console.aws.amazon.com/batch/home"
            f"?region={config['Region']}#jobs/fargate/detail/{response['jobId']}"
        )
        logging.info(f"Job submission request sent: '{job_url}'")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
