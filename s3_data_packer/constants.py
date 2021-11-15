import os

default_file_limit_gigabytes = 256 * 10 ** -3  # 256MB, up for debate
aws_region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "eu-west-1"))
