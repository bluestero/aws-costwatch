from datetime import datetime, timedelta, timezone

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import EC2UnusedConfig
from pipelines.base import BasePipeline


class EC2UnusedPipeline(BasePipeline):
    CONFIG = EC2UnusedConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")
        self.cw = session.client("cloudwatch")
        self.pricing = utils.EC2Pricing(session)

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.CONFIG.LOOKBACK_DAYS)

    # ----------------------
    # Private helpers
    # ----------------------
    def _get_max_metrics(self, instance_id: str) -> tuple[float, float, float]:
        period_seconds = 6 * 60 * 60  # 6 hours

        resp = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "cpu",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EC2",
                            "MetricName": "CPUUtilization",
                            "Dimensions": [{"Name": "InstanceId", "Value": instance_id}],
                        },
                        "Period": period_seconds,
                        "Stat": "Maximum",
                    },
                },
                {
                    "Id": "netin",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EC2",
                            "MetricName": "NetworkIn",
                            "Dimensions": [{"Name": "InstanceId", "Value": instance_id}],
                        },
                        "Period": period_seconds,
                        "Stat": "Maximum",
                    },
                },
                {
                    "Id": "netout",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EC2",
                            "MetricName": "NetworkOut",
                            "Dimensions": [{"Name": "InstanceId", "Value": instance_id}],
                        },
                        "Period": period_seconds,
                        "Stat": "Maximum",
                    },
                },
            ],
            StartTime=self.start_time,
            EndTime=self.end_time,
        )

        results = {r["Id"]: r.get("Values", []) for r in resp.get("MetricDataResults", [])}

        cpu = max(results.get("cpu", []), default=0.0)
        netin = max(results.get("netin", []), default=0.0)
        netout = max(results.get("netout", []), default=0.0)

        return cpu, netin, netout

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Fetching EC2 instances.")
        paginator = self.ec2.get_paginator("describe_instances")

        instances = []
        for page in paginator.paginate():
            for reservation in page.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    state = instance.get("State", {}).get("Name")
                    if state in {"terminated", "shutting-down", "stopping", "stopped"}:
                        continue

                    instances.append(instance)

        return instances

    def process_item(self, instance: dict) -> bool:
        instance_id = instance["InstanceId"]
        state = instance["State"]["Name"].upper()
        launch_time = instance["LaunchTime"]

        # Skip instances newer than lookback window (+1 day buffer)
        min_age = timedelta(days=self.CONFIG.LOOKBACK_DAYS + 1)
        if self.end_time - launch_time < min_age:
            return False

        name = next((t["Value"] for t in instance.get("Tags", []) if t["Key"] == "Name"), "")
        lifecycle = instance.get("InstanceLifecycle", "on-demand")
        instance_type = instance["InstanceType"]

        max_cpu = max_net_in = max_net_out = 0.0

        if state == "RUNNING":
            max_cpu, max_net_in, max_net_out = self._get_max_metrics(instance_id)

            if (
                max_cpu >= self.CONFIG.CPU_IDLE_THRESHOLD_PERCENTAGE
                or max_net_in >= self.CONFIG.NET_IDLE_THRESHOLD_MB
                or max_net_out >= self.CONFIG.NET_IDLE_THRESHOLD_MB
            ):
                return False

        status = "IDLE" if state == "RUNNING" else state
        created_at = launch_time.strftime("%Y-%m-%d %H:%M:%S")

        hourly_price = 0.0
        if status == "IDLE":
            hourly_price = self.pricing.get_hourly_price(instance)

        row = [
            instance_id,
            name,
            instance_type,
            lifecycle,
            status,
            created_at,
            round(max_cpu, 2),
            round(max_net_in / (1024 * 1024), 2),
            round(max_net_out / (1024 * 1024), 2),
            round(hourly_price, 4),
        ]

        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True
