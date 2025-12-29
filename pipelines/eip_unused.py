# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import EIPUnusedConfig


class EIPUnusedPipeline:
    def __init__(self):

        #-Clients-#
        self.session = utils.create_boto3_session()
        self.ec2 = self.session.client("ec2")

        #-Writing headers-#
        utils.write_to_csv(
            EIPUnusedConfig.OUTPUT_CSV,
            EIPUnusedConfig.CSV_HEADERS,
            mode="w"
        )


    def _fetch_all_eips(self):
        response = self.ec2.describe_addresses()
        return response.get("Addresses", [])


    def _is_attached_to_running_instance(self, instance_id: str) -> bool:
        response = self.ec2.describe_instances(InstanceIds=[instance_id])
        instance = response["Reservations"][0]["Instances"][0]
        return instance["State"]["Name"] == "running"


    def _process_eip(self, eip: dict):
        instance_id = eip.get("InstanceId")
        association_id = eip.get("AssociationId")
        network_interface_id = eip.get("NetworkInterfaceId")

        # Skip EIP associated with any NAT Gateway
        if association_id or network_interface_id:
            return None

        # Skip EIP attached to a running instance
        if instance_id and self._is_attached_to_running_instance(instance_id):
            return None

        row = [eip["PublicIp"], eip["AllocationId"]]

        utils.write_to_csv(EIPUnusedConfig.OUTPUT_CSV, row, mode="a")
        return row


    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Fetching Elastic IPs...")
        eips = self._fetch_all_eips()
        print(f"Found {len(eips)} Elastic IPs")

        count = 0
        for eip in eips:
            if self._process_eip(eip):
                count += 1

        print(f"Found {count} unused Elastic IPs")
        print(f"Report written to {EIPUnusedConfig.OUTPUT_CSV}.")
