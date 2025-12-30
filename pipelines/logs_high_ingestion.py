import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import LogsHighIngestionConfig


class LogsHighIngestionPipeline:
    PERIOD_DAYS = 30
    MAX_WORKERS = 8

    def __init__(self):

        # Clients
        session = utils.create_boto3_session()
        self.logs = session.client("logs")
        self.cw = session.client("cloudwatch")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.PERIOD_DAYS)

        # Writing headers
        utils.write_to_csv(LogsHighIngestionConfig.OUTPUT_CSV, LogsHighIngestionConfig.CSV_HEADERS, mode="w")

    # ----------------------
    # Private helpers
    # ----------------------
    def _fetch_log_groups(self):
        paginator = self.logs.get_paginator("describe_log_groups")
        for page in paginator.paginate():
            for lg in page.get("logGroups", []):
                yield lg

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

    def _process_log_group(self, lg: dict):
        log_group = lg["logGroupName"]
        monthly_ingested_bytes = self._get_monthly_ingested_bytes(log_group)
        monthly_ingested_gb = monthly_ingested_bytes / 1_000_000_000

        # Skip if below threshold
        if monthly_ingested_gb < LogsHighIngestionConfig.INGESTION_THRESHOLD_GB:
            return False

        row = [log_group, round(monthly_ingested_gb, 2)]
        utils.write_to_csv(LogsHighIngestionConfig.OUTPUT_CSV, row, mode="a")
        return True

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Scanning CloudWatch Log Groups for high ingestion.")
        count = 0
        log_groups = list(self._fetch_log_groups())
        print(f"Processing {len(log_groups)} log groups.")

        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_log_group, lg) for lg in log_groups]
            for future in as_completed(futures):
                if future.result():
                    count += 1

        print(f"Found {count} log groups with high ingestion.")
        print(f"Report written to {LogsHighIngestionConfig.OUTPUT_CSV}")
