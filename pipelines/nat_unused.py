import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import NATUnusedConfig


class NATUnusedPipeline:
    def __init__(self):

        # Clients
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")
        self.cw = session.client("cloudwatch")

        # Writing headers
        utils.write_to_csv(NATUnusedConfig.OUTPUT_CSV, NATUnusedConfig.CSV_HEADERS, mode="w")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=NATUnusedConfig.LOOKBACK_DAYS)

    # ----------------------
    # Private helpers
    # ----------------------
    def _is_nat_idle(self, nat_id: str) -> bool:
        metrics_to_check = [
            "ActiveConnectionCount",
            "BytesOutToDestination",
            "BytesInFromDestination",
        ]

        for metric_name in metrics_to_check:
            resp = self.cw.get_metric_statistics(
                Namespace="AWS/NATGateway",
                MetricName=metric_name,
                Dimensions=[{"Name": "NatGatewayId", "Value": nat_id}],
                StartTime=self.start_time,
                EndTime=self.end_time,
                Period=86400,  # 1 day
                Statistics=["Sum"],
            )

            for dp in resp.get("Datapoints", []):
                if dp.get("Sum", 0) > 0:
                    return False
        return True

    def _fetch_nat_gateways(self):
        """Fetch all NAT Gateways in the account."""
        return self.ec2.describe_nat_gateways().get("NatGateways", [])

    def _process_nat(self, nat: dict):
        nat_id = nat["NatGatewayId"]
        if not self._is_nat_idle(nat_id):
            return False

        row = [
            nat_id,
            nat["VpcId"],
            nat["State"],
            nat.get("SubnetId", "N/A"),
            nat["CreateTime"].strftime("%Y-%m-%d %H:%M:%S"),
        ]
        utils.write_to_csv(NATUnusedConfig.OUTPUT_CSV, row, mode="a")
        return True

    def _sort_csv(self):
        df = pd.read_csv(NATUnusedConfig.OUTPUT_CSV, encoding="utf-8")
        df.sort_values("Created Time", inplace=True)
        df.to_csv(NATUnusedConfig.OUTPUT_CSV, index=False)

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Fetching all NAT Gateways.")
        count = 0
        nat_gateways = self._fetch_nat_gateways()
        print(f"Processing {len(nat_gateways)} NAT Gateways.")

        with ThreadPoolExecutor(max_workers=NATUnusedConfig.MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_nat, nat) for nat in nat_gateways]
            for future in as_completed(futures):
                if future.result():
                    count += 1

        self._sort_csv()
        print(f"Found {count} idle NAT Gateways.")
        print(f"Report written to {NATUnusedConfig.OUTPUT_CSV}.")
