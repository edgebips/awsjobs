# Aws Batch Job Utilities
## Overview

I want to run a lot of distributed batch jobs on AWS Batch with AWS Fargate Spot
instances (very cheap, but interruptible). To do that, you need to

- Build a docker container around your command
- Upload the container to ECR
- Create a compute environment
- Create a job queue
- Create a job definition
- Submit a job
- Tear down everything after you're done

And some more. This is what these scripts do.

- `docker_upload.py` can build a docker container around a command
- `aws_batch.py` can be used to setup, submit (many times) and teardown the
  jobs.
- `aws_dispatcher.py` is a little trampoline script that is embedded inside a
  Docker container to read the mapped input (this is an "array job") and invoke
  the actual command with this input as stdin.

## Usage

Configure:

    Create a file like config.json

Upload a docker container to be run (you have to build it first):

    docker_upload.py <docker-image-tag>

Or, build and upload docker container around a Python command:

    docker_upload.py <docker-image-tag> --build <program> <arguments>

Setup AWS resources:

    aws_batch.py setup

Submit a job:

    aws_batch.py submit <docker-image-tag> <input-file>

The input file has one line per worker. The input will be provided to the
command as stdin.

Teardown AWS resources:

    aws_batch.py teardown
