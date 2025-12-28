import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import EC2IdleConfig

class EC2IdlePipeline:
    CPU_IDLE_THRESHOLD_PERCENTAGE = 2.0
    NET_IDLE_THRESHOLD_MB = 5 * 1024 * 1024

    def __init__(self):

        #-Clients-#
        self.session = utils.create_boto3_session()
        self.ec2 = self.session.client("ec2")
        self.cloudwatch = self.session.client("cloudwatch")

        #-Writing headers-#
        utils.write_to_csv(EC2IdleConfig.OUTPUT_CSV, EC2IdleConfig.CSV_HEADERS, mode="w")

    # ----------------------
    # Private helpers
    # ----------------------
    def _fetch_all_instances(self):
        paginator = self.ec2.get_paginator("describe_instances")
        instances = []

        for page in paginator.paginate():
            for reservation in page.get("Reservations", []):
                for inst in reservation.get("Instances", []):
                    instances.append({
                        "InstanceId": inst["InstanceId"],
                        "Name": next((t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"), ""),
                        "Type": inst["InstanceType"],
                        "Lifecycle": inst.get("InstanceLifecycle", "on-demand"),
                        "State": inst["State"]["Name"].upper(),
                        "LaunchTime": inst["LaunchTime"].strftime("%Y-%m-%d %H:%M:%S")
                    })
        return instances


    def _get_max_metric(self, instance_id, metric_name, unit):
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days = EC2IdleConfig.NUMBER_OF_DAYS)

        resp = self.cloudwatch.get_metric_statistics(
            Namespace="AWS/EC2",
            MetricName=metric_name,
            Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
            StartTime=start_time,
            EndTime=end_time,
            Period=EC2IdleConfig.NUMBER_OF_DAYS * 24 * 3600,
            Statistics=["Maximum"],
            Unit=unit
        )
        datapoints = resp.get("Datapoints", [])
        return max(dp["Maximum"] for dp in datapoints) if datapoints else 0.0


    def _process_instance(self, instance):
        state = instance["State"]
        max_cpu = max_net_in = max_net_out = 0.0

        if state == "RUNNING":
            max_cpu = self._get_max_metric(instance["InstanceId"], "CPUUtilization", "Percent")
            max_net_in = self._get_max_metric(instance["InstanceId"], "NetworkIn", "Bytes")
            max_net_out = self._get_max_metric(instance["InstanceId"], "NetworkOut", "Bytes")
            status = "IDLE" if (max_cpu < self.CPU_IDLE_THRESHOLD_PERCENTAGE and
                                max_net_in < self.NET_IDLE_THRESHOLD_MB and
                                max_net_out < self.NET_IDLE_THRESHOLD_MB) else "ACTIVE"
        else:
            status = state

        row = [
            instance["InstanceId"],
            instance["Name"],
            instance["Type"],
            instance["Lifecycle"],
            instance["State"],
            instance["LaunchTime"],
            status,
            round(max_cpu, 2),
            round(max_net_in / (1024 * 1024), 2),
            round(max_net_out / (1024 * 1024), 2)
        ]

        utils.write_to_csv(EC2IdleConfig.OUTPUT_CSV, row, mode="a")
        return row

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Fetching EC2 instances...")
        instances = self._fetch_all_instances()
        print(f"Found {len(instances)} instances")

        print("Processing instances in parallel...")
        with ThreadPoolExecutor(max_workers=EC2IdleConfig.MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_instance, inst) for inst in instances]
            for f in as_completed(futures):
                f.result()

        print(f"Done → {EC2IdleConfig.OUTPUT_CSV}")
