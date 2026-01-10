from datetime import datetime, timedelta, timezone

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import LogsNeverExpireConfig
from pipelines.base import BasePipeline


class LogsNeverExpirePipeline(BasePipeline):
    CONFIG = LogsNeverExpireConfig
    PERIOD_DAYS = 30

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.logs = session.client("logs")
        self.cw = session.client("cloudwatch")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.PERIOD_DAYS)

    # ----------------------
    # Private helpers
    # ----------------------
    def _get_monthly_ingested_bytes(self, log_group_name: str) -> int:
        response = self.cw.get_metric_statistics(
            Namespace="AWS/Logs",
            MetricName="IncomingBytes",
            Dimensions=[{"Name": "LogGroupName", "Value": log_group_name}],
            StartTime=self.start_time,
            EndTime=self.end_time,
            Period=86400,
            Statistics=["Sum"],
        )

        return int(sum(dp.get("Sum", 0) for dp in response.get("Datapoints", [])))

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Scanning CloudWatch Log Groups (Never Expire).")

        paginator = self.logs.get_paginator("describe_log_groups")
        log_groups = []

        for page in paginator.paginate():
            for lg in page.get("logGroups", []):
                # Only log groups with no retention policy
                if "retentionInDays" not in lg:
                    log_groups.append(lg)

        return log_groups

    def process_item(self, lg: dict) -> bool:
        log_group = lg["logGroupName"]
        stored_bytes = lg.get("storedBytes", 0)
        monthly_ingested_bytes = self._get_monthly_ingested_bytes(log_group)

        row = [log_group, round(stored_bytes / 1_000_000_000, 2), round(monthly_ingested_bytes / 1_000_000_000, 2)]

        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True
