import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import EBSUnusedConfig


class EBSUnusedPipeline:
    def __init__(self):

        # Clients
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")
        self.cw = session.client("cloudwatch")

        # Initialize CSV with headers
        utils.write_to_csv(EBSUnusedConfig.OUTPUT_CSV, EBSUnusedConfig.CSV_HEADERS, mode="w")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=EBSUnusedConfig.LOOKBACK_DAYS)

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

    def _is_volume_active(self, vol_id: str) -> bool:
        """Returns True if there is any read/write I/O for this volume in the lookback period."""
        resp = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "r",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeReadOps",
                            "Dimensions": [{"Name": "VolumeId", "Value": vol_id}],
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
                            "Dimensions": [{"Name": "VolumeId", "Value": vol_id}],
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

        for result in resp["MetricDataResults"]:
            if any(v > 0 for v in result.get("Values", [])):
                return True
        return False

    def _fetch_volumes(self):
        response = self.ec2.describe_volumes()
        return response.get("Volumes", [])

    def _process_volume(self, volume: dict):
        tags = volume.get("Tags", [])
        volume_id = volume["VolumeId"]

        # Skip Kubernetes-managed volumes
        if self._is_kubernetes_volume(tags):
            return False

        # Skip explicitly protected volumes
        if self._is_protected_volume(tags):
            return False

        # Skip if volume is active
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
        utils.write_to_csv(EBSUnusedConfig.OUTPUT_CSV, row, mode="a")
        return True

    def _sort_csv(self):
        self.df = pd.read_csv(EBSUnusedConfig.OUTPUT_CSV, encoding="utf-8")
        self.df.sort_values(EBSUnusedConfig.SORT_BY_COLUMN).to_csv(EBSUnusedConfig.OUTPUT_CSV, index=False)

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Fetching all EBS volumes.")
        count = 0
        volumes = self._fetch_volumes()
        print(f"Processing {len(volumes)} EBS volumes.")

        with ThreadPoolExecutor(max_workers=EBSUnusedConfig.MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_volume, volume) for volume in volumes]
            for future in as_completed(futures):
                if future.result():
                    count += 1

        self._sort_csv()
        if EBSUnusedConfig.WRITE_TO_GOOGLE_SHEET:
            utils.write_df_to_sheet(EBSUnusedConfig.WORKSHEET_NAME, self.df)

        print(f"Found {count} unused EBS volumes.")
        print(f"Report written to {EBSUnusedConfig.OUTPUT_CSV}.")
