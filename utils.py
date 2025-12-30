import csv
import json
import boto3
import configparser
from pathlib import Path

# ----------------------
# Custom Imports
# ----------------------
from settings import CommonConfig

# -------------------------------------------
# AWS Boto3 Session
# -------------------------------------------
def create_boto3_session(
    credentials_file: Path = Path("./credentials")
) -> boto3.Session:
    """
    Create a boto3 session using a local credentials file if it exists,
    otherwise fall back to default AWS credential resolution.
    """
    session_kwargs = {"region_name": CommonConfig.AWS_REGION}

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

# -------------------------------------------
# AWS EC2 Price Fetcher
# -------------------------------------------
class EC2Pricing:
    REGION_NAME_MAP = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
    }

    def __init__(self, boto3_session=None):
        session = boto3_session or boto3.Session()

    def get_on_demand_price(self, instance_type: str, region: str = "us-east-1", os: str = "Linux") -> float:
        location = self.REGION_NAME_MAP.get(region, "US East (N. Virginia)")
        pricing_client = session.client("pricing", region_name="us-east-1")

        response = pricing_client.get_products(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": os},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            ],
            MaxResults=1
        )

        price_list = [json.loads(p) for p in response["PriceList"]]
        try:
            terms = price_list[0]["terms"]["OnDemand"]
            for sku in terms:
                price_dimensions = terms[sku]["priceDimensions"]
                for pd in price_dimensions:
                    hourly_price = float(price_dimensions[pd]["pricePerUnit"]["USD"])
                    return hourly_price
        except Exception as e:
            return f"Error parsing  price: {e}."

    def get_spot_price(self, instance_type: str, region: str = "us-east-1", os: str = "Linux") -> float:
        ec2_client = session.client("ec2", region_name=region)
        try:
            spot_history = ec2_client.describe_spot_price_history(
                InstanceTypes=[instance_type],
                ProductDescriptions=[f"{os}/UNIX"],
                MaxResults=1
            )
            hourly_price = float(spot_history["SpotPriceHistory"][0]["SpotPrice"])
            return hourly_price
        except Exception as e:
            return f"Error fetching spot price: {e}"

    def get_hourly_price(self, instance_type: str, lifecycle: str, region: str = "us-east-1", os: str = "Linux") -> float:
        lifecycle = lifecycle.lower()
        if lifecycle == "on-demand":
            return self.get_on_demand_price(instance_type, region, os)
        elif lifecycle == "spot":
            return self.get_spot_price(instance_type, region, os)
        else:
            return "Unsupported lifecycle. Use 'on-demand' or 'spot'."
