import pandas as pd

# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import EBSUnusedConfig


class EBSUnusedPipeline:
    def __init__(self):

        # Clients
        self.session = utils.create_boto3_session()
        self.ec2 = self.session.client("ec2")

        # Initialize CSV with headers
        utils.write_to_csv(EBSUnusedConfig.OUTPUT_CSV, EBSUnusedConfig.CSV_HEADERS, mode="w")

    # ----------------------
    # Private helpers
    # ----------------------
    def _fetch_unused_volumes(self):
        response = self.ec2.describe_volumes(
            Filters=[{"Name": "status", "Values": ["available"]}]
        )
        return response.get("Volumes", [])

    def _process_volume(self, volume: dict):
        volume_id = volume["VolumeId"]
        size_gb = volume["Size"]
        volume_type = volume["VolumeType"]
        create_time = (
            volume["CreateTime"].strftime("%Y-%m-%d %H:%M:%S")
            if hasattr(volume["CreateTime"], "strftime")
            else volume["CreateTime"]
        )

        row = [volume_id, size_gb, volume_type, create_time]

        utils.write_to_csv(EBSUnusedConfig.OUTPUT_CSV, row, mode="a")

    def _sort_csv(self):
        df = pd.read_csv(EBSUnusedConfig.OUTPUT_CSV, encoding = "utf-8")
        df.sort_values(EBSUnusedConfig.SORT_BY_COLUMN).to_csv(EBSUnusedConfig.OUTPUT_CSV, index = False)

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Fetching unused EBS volumes.")
        volumes = self._fetch_unused_volumes()
        print(f"Found {len(volumes)} unused volumes.")

        for volume in volumes:
            self._process_volume(volume)

        print(f"Report written to {EBSUnusedConfig.OUTPUT_CSV}.")
