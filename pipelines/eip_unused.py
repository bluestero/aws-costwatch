# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import EIPUnusedConfig
from pipelines.base import BasePipeline


class EIPUnusedPipeline(BasePipeline):
    CONFIG = EIPUnusedConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")

    # ----------------------
    # Private helpers
    # ----------------------
    def _is_attached_to_running_instance(self, instance_id: str) -> bool:
        response = self.ec2.describe_instances(InstanceIds=[instance_id])
        instance = response["Reservations"][0]["Instances"][0]
        return instance["State"]["Name"] == "running"

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Fetching Elastic IPs.")
        response = self.ec2.describe_addresses()
        return response.get("Addresses", [])

    def process_item(self, eip: dict) -> bool:
        instance_id = eip.get("InstanceId")
        association_id = eip.get("AssociationId")
        network_interface_id = eip.get("NetworkInterfaceId")

        # Skip EIP associated with any NAT Gateway
        if association_id or network_interface_id:
            return False

        # Skip EIP attached to a running instance
        if instance_id and self._is_attached_to_running_instance(instance_id):
            return False

        row = [eip["PublicIp"], eip["AllocationId"]]
        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True
