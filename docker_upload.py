#!/usr/bin/env python3
"""Upload a local Docker container to ECR."""

import argparse
import base64
import shutil
import io
from os import path
import logging
import os
import tempfile
import textwrap
from functools import partial

import boto3
from botocore.client import BaseClient
import docker


def create_dockerfile(
    command: list[str], direct: bool, region: str
) -> tuple[str, list[str]]:
    """Create a dockerfile contents for the given command."""
    buf = io.StringIO()
    pr = partial(print, file=buf)

    session = boto3.Session()
    credentials = session.get_credentials()
    aws_region = session.region_name or "us-east-1"
    current_credentials = credentials.get_frozen_credentials()
    aws_access_key_id = current_credentials.access_key
    aws_secret_access_key = current_credentials.secret_key
    aws_session_token = current_credentials.token

    pr(
        textwrap.dedent("""\
        FROM python:3.13-slim

        RUN pip install -U pip
        RUN pip install --root-user-action=ignore boto3
        RUN pip install --root-user-action=ignore duckdb
        RUN pip install --root-user-action=ignore polars

        RUN adduser --disabled-password --quiet --gecos "" mapuser
        USER mapuser
        WORKDIR /home/mapuser
    """)
    )

    # Add AWS credentials to the Dockerfile.
    # Note: This doesn't appear necessary: ENV AWS_SESSION_TOKEN {aws_session_token}
    pr(
        textwrap.dedent(f"""\
        ENV AWS_ACCESS_KEY_ID		{aws_access_key_id}
        ENV AWS_SECRET_ACCESS_KEY	{aws_secret_access_key}
        ENV AWS_REGION                  {aws_region}
    """)
    )
    pr()
    filenames = [path.join(path.dirname(__file__), "aws_dispatcher.py")]
    pr(f"COPY aws_dispatcher.py aws_dispatcher.py")
    for arg in command:
        if path.exists(arg):
            filenames.append(arg)
            pr(f"COPY {arg} {arg}")
    pr()

    command_str = " ".join(command)
    if direct:
        pr(f"CMD {command_str}")
    else:
        pr(f"ENV BATCH_JOB_COMMAND {command_str}")
        pr(f"CMD python aws_dispatcher.py")
    return buf.getvalue(), filenames


def build_docker_container(
    image_tag: str, command: list[str], direct: bool, region: str
):
    # Create Dockerfile to build.
    dockerfile_content, filenames = create_dockerfile(command, direct, region)

    with tempfile.TemporaryDirectory(prefix="docker-build.") as tmpdir:
        # Createing tempdir for docker context.
        os.makedirs(tmpdir, exist_ok=True)
        with open(os.path.join(tmpdir, "Dockerfile"), "w") as f:
            f.write(textwrap.dedent(dockerfile_content))

        for filename in filenames:
            shutil.copy(filename, path.join(tmpdir, path.basename(filename)))

        client = docker.from_env()
        print(f"Building Docker image: {image_tag} from path: {tmpdir}")
        try:
            image, build_log = client.images.build(
                path=tmpdir,
                tag=image_tag,
                rm=True,  # Remove intermediate containers
                forcerm=True,  # Always remove intermediate containers
                quiet=logging.getLogger().getEffectiveLevel() < logging.INFO,
            )
            print(f"Image {image.id} built successfully!")
            print("Build log:")
            for chunk in build_log:
                if "stream" in chunk:
                    print(chunk["stream"].strip())
        except docker.errors.BuildError as e:
            print(f"Error building image: {e}")
            print("Full build log:")
            for log_entry in e.build_log:
                if "stream" in log_entry:
                    print(log_entry["stream"].strip())
        except docker.errors.APIError as e:
            print(f"Docker API Error: {e}")

    return image_tag


def ensure_ecr_repository_exists(ecr_client: BaseClient, repository: str):
    """
    Ensures that an ECR repository with the given name exists.
    If it doesn't exist, it creates one.
    """
    try:
        ecr_client.describe_repositories(repositoryNames=[repository])
        logging.info(f"ECR repository '{repository}' already exists.")
    except ecr_client.exceptions.RepositoryNotFoundException:
        logging.info(f"ECR repository '{repository}' not found. Creating it...")
        ecr_client.create_repository(repositoryName=repository)
        logging.info(f"Successfully created ECR repository '{repository}'.")


def create_docker_client(ecr_client) -> tuple[str, str]:
    """Create and login a docker client from the ECR client."""

    auth_data = ecr_client.get_authorization_token()
    authorization_token = auth_data["authorizationData"][0]["authorizationToken"]
    https_registry = auth_data["authorizationData"][0]["proxyEndpoint"]
    registry = https_registry.replace("https://", "")

    decoded_token = base64.b64decode(authorization_token).decode("utf-8")
    username, password = decoded_token.split(":")

    docker_config = {"username": username, "password": password}
    docker_client = docker.from_env()
    docker_client.login(registry=https_registry, **docker_config)

    return docker_client, registry


def upload_docker_image_to_ecr(image: str, region: str) -> str:
    """
    Uploads a local Docker image to an AWS ECR repository.
    """

    # Determine image repository and tag
    if ":" in image:
        repository, image_tag = image.split(":", 1)
    else:
        repository, image_tag = image, "latest"

    # 0. Ensure that the repository exists.
    ecr_client = boto3.client("ecr", region_name=region)
    ensure_ecr_repository_exists(ecr_client, repository)

    # 1. Authenticate Docker Configuration
    docker_client, registry = create_docker_client(ecr_client)
    logging.info(f"Docker login successful for registry: {registry}")

    # 2. Tag the local Docker image
    try:
        local_image = docker_client.images.get(f"{repository}:{image_tag}")
        ecr_image_uri = f"{registry}/{repository}:{image_tag}"
        local_image.tag(repository=f"{registry}/{repository}", tag=image_tag)
        logging.info(f"Tagged local image '{local_image}' as '{ecr_image_uri}'")
    except docker.errors.ImageNotFound:
        logging.error(f"Error: Local image in repository '{repository}' not found.")
        return

    # 3. Push the Docker image to ECR
    logging.info(f"Pushing image '{ecr_image_uri}'...")
    for line in docker_client.images.push(
        repository=f"{registry}/{repository}", tag=image_tag, stream=True
    ):
        logging.info(line.decode("utf-8").strip())
    logging.info(f"Successfully pushed image '{ecr_image_uri}' to ECR")

    return ecr_image_uri


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Upload a Docker image to AWS ECR.")
    parser.add_argument(
        "docker_image",
        help=(
            "The name of the local Docker image to upload, optionally tagged"
            " (e.g., 'my-app:latest')."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="The region.",
    )
    parser.add_argument(
        "-b",
        "-c",
        "--build",
        action="store",
        nargs=argparse.REMAINDER,
        help="A command to build the Docker image around.",
    )
    parser.add_argument(
        "-D",
        "--direct",
        action="store_true",
        help="Run the script directly with no trampoline.",
    )
    args = parser.parse_args()

    # Build the container if a script is provided.
    if args.build:
        command = args.build.split() if isinstance(args.build, str) else args.build
        image = build_docker_container(
            args.docker_image, command, args.direct, args.region
        )
    else:
        image = args.docker_image
    logging.info(f"Image {image} built successfully!")

    # Upload the container to the registry.
    ecr_image_uri = upload_docker_image_to_ecr(args.docker_image, args.region)
    logging.info("ECR image URI: %s", ecr_image_uri)


if __name__ == "__main__":
    main()
