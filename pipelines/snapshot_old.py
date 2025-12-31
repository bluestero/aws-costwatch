from datetime import datetime, timezone
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import SnapshotOldConfig


class SnapshotOldPipeline:
    def __init__(self):

        # Clients
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")

        # Time range
        self.cutoff = datetime.fromisoformat(SnapshotOldConfig.SNAPSHOT_CUTOFF_DATE).replace(tzinfo=timezone.utc)

        # Writing headers
        utils.write_to_csv(SnapshotOldConfig.OUTPUT_CSV, SnapshotOldConfig.CSV_HEADERS, mode="w")

    # ----------------------
    # Private helpers
    # ----------------------
    def _fetch_snapshots(self):
        response = self.ec2.describe_snapshots(OwnerIds=["self"])
        return response.get("Snapshots", [])

    def _process_snapshot(self, snap: dict):
        snap_time = snap["StartTime"]

        if snap_time >= self.cutoff:
            return False

        snapshot_id = snap["SnapshotId"]
        volume_id = snap.get("VolumeId")
        size_gb = snap.get("VolumeSize", 0)
        snapshot_date = snap_time.date().isoformat()

        volume_name = ""
        volume_type = ""
        instance_id = ""
        instance_name = ""

        try:
            vol = self.ec2.describe_volumes(VolumeIds=[volume_id])["Volumes"][0]
            volume_type = vol.get("VolumeType", "")

            volume_name = next((t["Value"] for t in vol.get("Tags", []) if t["Key"] == "Name"), "")

            if vol.get("Attachments"):
                instance_id = vol["Attachments"][0]["InstanceId"]

                inst = self.ec2.describe_instances(InstanceIds=[instance_id])
                instance = inst["Reservations"][0]["Instances"][0]

                instance_name = next((t["Value"] for t in instance.get("Tags", []) if t["Key"] == "Name"), "")

        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidVolume.NotFound":
                volume_name = "DeletedVolume"
                volume_type = "Unknown"
                instance_id = "N/A"
                instance_name = "N/A"
            else:
                raise

        row = [
            snapshot_id,
            volume_id,
            volume_name,
            volume_type,
            instance_id,
            instance_name,
            size_gb,
            snapshot_date,
        ]

        utils.write_to_csv(SnapshotOldConfig.OUTPUT_CSV, row, mode="a")
        return True

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        logger.info("Fetching snapshots.")
        count = 0
        snapshots = self._fetch_snapshots()
        logger.info(f"Processing {len(snapshots)} snapshots.")

        with ThreadPoolExecutor(max_workers=SnapshotOldConfig.MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_snapshot, snapshot) for snapshot in snapshots]
            for future in as_completed(futures):
                if future.result():
                    count += 1

        logger.info(f"Snapshots older than {SnapshotOldConfig.SNAPSHOT_CUTOFF_DATE}: {count}.")
        logger.info(f"Report written to {SnapshotOldConfig.OUTPUT_CSV}.")
