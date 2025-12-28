import csv
import boto3
import configparser
from pathlib import Path


# -------------------------------------------
# AWS Boto3 Session
# -------------------------------------------
def create_boto3_session(
    credentials_file: Path = Path("./credentials"),
    region_name: str = "us-east-1",
) -> boto3.Session:
    """
    Create a boto3 session using a local credentials file if it exists,
    otherwise fall back to default AWS credential resolution.
    """
    session_kwargs = {"region_name": region_name}

    if credentials_file.exists():
        config = configparser.ConfigParser()
        config.read(credentials_file)

        profile_name = config.sections()[0]
        profile_config = config[profile_name]

        print("Credentials file found, using custom credentials.")

        session_kwargs.update(
            {
                "aws_access_key_id": profile_config.get("aws_access_key_id"),
                "aws_secret_access_key": profile_config.get("aws_secret_access_key"),
                "aws_session_token": profile_config.get("aws_session_token"),
            }
        )

    return boto3.Session(**session_kwargs)

# -------------------------------------------
# CSV Writer
# -------------------------------------------
def write_to_csv(file: str, row: list[str], mode: str):
    Path(file).parent.mkdir(parents=True, exist_ok=True)
    with open(file, mode, newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)
