import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import EC2IdleConfig, EC2UnusedConfig

class EC2UnusedPipeline:

    def __init__(self):

        # Clients
        self.session = utils.create_boto3_session()
        self.ec2 = self.session.client("ec2")
        self.pricing = utils.EC2Pricing()

        # Writing headers
        utils.write_to_csv(EC2UnusedConfig.OUTPUT_CSV, EC2UnusedConfig.CSV_HEADERS, mode="w")

    # ----------------------
    # Private helpers
    # ----------------------
    def _read_non_active_instances(self):
        instances = []
        with open(EC2IdleConfig.OUTPUT_CSV, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                instances.append(row)
        return instances

    def _process_instance(self, inst):
        status = inst["Status"]
        instance_id = inst["Instance ID"]
        instance_type = inst["Type"]
        lifecycle = inst.get("Lifecycle", "").lower()

        # ---------- EC2 ----------
        hourly_price = 0
        if status == "IDLE":
            hourly_price = self.pricing.get_hourly_price(instance_type, lifecycle)

        # ---------- EBS ----------
        volumes = self.ec2.describe_volumes(
            Filters=[{"Name": "attachment.instance-id", "Values": [instance_id]}]
        ).get("Volumes", [])
        volume_size_gb = sum(v["Size"] for v in volumes)

        # ---------- ELASTIC IP ----------
        addresses = self.ec2.describe_addresses(
            Filters=[{"Name": "instance-id", "Values": [instance_id]}]
        ).get("Addresses", [])
        eip_count = sum(1 for addr in addresses if not addr.get("AssociationId"))

        row = [
            instance_id,
            inst["Name"],
            instance_type,
            lifecycle,
            status,
            hourly_price,
            len(volumes),
            round(volume_size_gb, 2),
            eip_count,
        ]

        utils.write_to_csv(EC2UnusedConfig.OUTPUT_CSV, row, mode="a")

    def _sort_csv(self):
        df = pd.read_csv(EC2UnusedConfig.OUTPUT_CSV, encoding = "utf-8")
        df = df.sort_values(EC2UnusedConfig.SORT_BY_COLUMN, ascending = False)
        df.to_csv(EC2UnusedConfig.OUTPUT_CSV, index = False)

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Reading inactive EC2 instances.")
        instances = self._read_non_active_instances()
        print(f"Processing {len(instances)} instances.")

        with ThreadPoolExecutor(max_workers=EC2UnusedConfig.MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_instance, inst) for inst in instances]
            for future in as_completed(futures):
                future.result()

        self._sort_csv()
        print(f"Report written to {EC2UnusedConfig.OUTPUT_CSV}")
