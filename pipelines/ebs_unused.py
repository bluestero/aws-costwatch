from datetime import datetime, timezone, timedelta

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import EBSUnusedConfig
from pipelines.base import BasePipeline


class EBSUnusedPipeline(BasePipeline):
    CONFIG = EBSUnusedConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")
        self.cw = session.client("cloudwatch")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.CONFIG.LOOKBACK_DAYS)

    # ----------------------
    # Private helpers
    # ----------------------
    def _is_protected_volume(self, tags: list) -> bool:
        if not tags:
            return False

        protected_keys = {"keep", "do_not_delete", "protected"}

        return any(tag["Key"].lower() in protected_keys for tag in tags)

    def _is_kubernetes_volume(self, tags: list) -> bool:
        if not tags:
            return False

        k8s_indicators = ("kubernetes.io/", "ebs.csi.aws.com", "csivolumename")

        for tag in tags:
            key = tag["Key"].lower()
            if any(indicator in key for indicator in k8s_indicators):
                return True

        return False

    def _is_volume_active(self, volume_id: str) -> bool:
        resp = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "r",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeReadOps",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 86400,
                        "Stat": "Sum",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "w",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeWriteOps",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 86400,
                        "Stat": "Sum",
                    },
                    "ReturnData": True,
                },
            ],
            StartTime=self.start_time,
            EndTime=self.end_time,
        )

        for result in resp.get("MetricDataResults", []):
            if any(v > 0 for v in result.get("Values", [])):
                return True

        return False

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Fetching all EBS volumes.")
        response = self.ec2.describe_volumes()
        return response.get("Volumes", [])

    def process_item(self, volume: dict) -> bool:
        tags = volume.get("Tags", [])
        volume_id = volume["VolumeId"]

        if self._is_kubernetes_volume(tags):
            return False

        if self._is_protected_volume(tags):
            return False

        if self._is_volume_active(volume_id):
            return False

        size_gb = volume["Size"]
        volume_type = volume["VolumeType"]
        create_time = (
            volume["CreateTime"].strftime("%Y-%m-%d %H:%M:%S")
            if hasattr(volume["CreateTime"], "strftime")
            else volume["CreateTime"]
        )

        row = [volume_id, size_gb, volume_type, create_time]
        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True
