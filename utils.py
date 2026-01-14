import csv
import json
import boto3
import gspread
import configparser
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# -------------------------------------------
# Logger
# -------------------------------------------
import logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s [%(filename)s]",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ----------------------
# Custom Imports
# ----------------------
from settings import CommonConfig


# -------------------------------------------
# AWS Boto3 Session
# -------------------------------------------
def create_boto3_session(credentials_file: Path = Path("./credentials")) -> boto3.Session:
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

        logger.debug("Credentials file found, using custom credentials.")

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
# Google Sheet Functions
# -------------------------------------------
def get_gspread_client():
    creds = Credentials.from_service_account_file(
        CommonConfig.GCC_JSON_PATH,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def get_worksheet(worksheet_name: str):
    gc = get_gspread_client()
    sheet = gc.open(CommonConfig.SPREADSHEET_NAME)
    return sheet.worksheet(worksheet_name)

def col_num_to_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def write_df_to_sheet(worksheet_name: str, df: pd.DataFrame):
    worksheet = get_worksheet(worksheet_name)
    values = df.values.tolist()
    num_columns = len(values[0])
    end_row = 2 + len(values) - 1
    end_col = col_num_to_letter(num_columns)
    clear_range = f"A2:{end_col}"
    cell_range = f"A2:{end_col}{end_row}"

    worksheet.batch_clear([clear_range])
    worksheet.update(cell_range, values, value_input_option="USER_ENTERED")

# -------------------------------------------
# AWS EC2 Price Fetcher
# -------------------------------------------
class EC2Pricing:
    REGION_NAME_MAP = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "eu-west-1": "EU (Ireland)",
        "eu-central-1": "EU (Frankfurt)",
        "ap-south-1": "Asia Pacific (Mumbai)",
    }

    def __init__(self, session: boto3.Session = None):
        session = session or create_boto3_session()
        self.pricing = session.client("pricing", region_name="us-east-1")
        self.ec2 = session.client("ec2")

        # Cache: (instance_type, region, os, lifecycle) -> hourly_price
        self._cache = {}

    # ----------------------
    # Public API
    # ----------------------
    def get_hourly_price(self, instance: dict) -> float:
        instance_type = instance["InstanceType"]

        az = instance["Placement"]["AvailabilityZone"]
        region = az[:-1]

        lifecycle = instance.get("InstanceLifecycle", "on-demand").lower()

        platform = instance.get("PlatformDetails", "Linux/UNIX")
        operating_system = "Windows" if "Windows" in platform else "Linux"

        cache_key = (instance_type, region, operating_system, lifecycle)
        if cache_key in self._cache:
            return self._cache[cache_key]

        if lifecycle == "spot":
            price = self._get_spot_price(instance_type, operating_system)
        else:
            has_license = bool(instance.get("ProductCodes"))
            price = self._get_on_demand_price(instance_type, region, operating_system, has_license)

        self._cache[cache_key] = price
        return price

    # ----------------------
    # Internal helpers
    # ----------------------
    def _get_on_demand_price(self, instance_type: str, region: str, os: str, has_license: bool) -> float:
        location = self.REGION_NAME_MAP.get(region)
        if not location:
            return 0.0

        paginator = self.pricing.get_paginator("get_products")

        for page in paginator.paginate(
            ServiceCode="AmazonEC2",
            Filters=[
                {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": os},
            ],
        ):
            for raw in page.get("PriceList", []):
                product = json.loads(raw)

                if product.get("product", {}).get("productFamily") != "Compute Instance":
                    continue

                attrs = product["product"]["attributes"]
                if attrs.get("tenancy") != "Shared":
                    continue

                terms = product.get("terms", {}).get("OnDemand", {})

                for sku in terms.values():
                    for dim in sku.get("priceDimensions", {}).values():
                        desc = dim.get("description", "")
                        usd = dim.get("pricePerUnit", {}).get("USD")

                        # Skip reserved usage
                        if "Reservation" in desc:
                            continue

                        # If instance is NOT licensed, skip licensed SKUs
                        if not has_license and " with " in desc:
                            continue

                        # If instance IS licensed, require licensed SKU
                        if has_license and " with " not in desc:
                            continue

                        if dim.get("unit") == "Hrs" and usd and float(usd) > 0:
                            return float(usd)

        return 0.0

    def _get_spot_price(self, instance_type: str, os: str) -> float:
        end = datetime.utcnow()
        start = end - timedelta(hours=24)

        product = "Windows" if os == "Windows" else "Linux/UNIX"

        resp = self.ec2.describe_spot_price_history(
            InstanceTypes=[instance_type],
            ProductDescriptions=[product],
            StartTime=start,
            EndTime=end,
        )

        prices = [float(p["SpotPrice"]) for p in resp.get("SpotPriceHistory", [])]
        if not prices:
            return 0.0

        return round(sum(prices) / len(prices), 4)
