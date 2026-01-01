from datetime import datetime, timedelta, timezone

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import LogsHighIngestionConfig
from pipelines.base import BasePipeline


class LogsHighIngestionPipeline(BasePipeline):
    CONFIG = LogsHighIngestionConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.logs = session.client("logs")
        self.cw = session.client("cloudwatch")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.CONFIG.LOOKBACK_DAYS)

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
        return int(sum(dp["Sum"] for dp in response.get("Datapoints", [])))

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Scanning CloudWatch Log Groups for high ingestion.")
        paginator = self.logs.get_paginator("describe_log_groups")

        log_groups = []
        for page in paginator.paginate():
            log_groups.extend(page.get("logGroups", []))

        return log_groups

    def process_item(self, lg: dict) -> bool:
        log_group = lg["logGroupName"]
        monthly_ingested_bytes = self._get_monthly_ingested_bytes(log_group)
        monthly_ingested_gb = monthly_ingested_bytes / 1_000_000_000

        if monthly_ingested_gb < self.CONFIG.INGESTION_THRESHOLD_GB:
            return False

        row = [log_group, round(monthly_ingested_gb, 2)]
        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True
