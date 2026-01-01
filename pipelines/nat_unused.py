from datetime import datetime, timedelta, timezone

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import NATUnusedConfig
from pipelines.base import BasePipeline


class NATUnusedPipeline(BasePipeline):
    CONFIG = NATUnusedConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")
        self.cw = session.client("cloudwatch")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=NATUnusedConfig.LOOKBACK_DAYS)

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Fetching all NAT Gateways.")
        return self.ec2.describe_nat_gateways().get("NatGateways", [])

    def process_item(self, nat: dict) -> bool:
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

        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True

    # -------------------------------
    # Private helpers
    # -------------------------------
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
                Period=86400,
                Statistics=["Sum"],
            )

            for dp in resp.get("Datapoints", []):
                if dp.get("Sum", 0) > 0:
                    return False

        return True
