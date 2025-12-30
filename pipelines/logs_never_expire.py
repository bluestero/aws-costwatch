import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import LogsNeverExpireConfig


class LogsNeverExpirePipeline:
    PERIOD_DAYS = 30

    def __init__(self):

        # Clients
        session = utils.create_boto3_session()
        self.logs = session.client("logs")
        self.cw = session.client("cloudwatch")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.PERIOD_DAYS)

        # Writing headers
        utils.write_to_csv(LogsNeverExpireConfig.OUTPUT_CSV, LogsNeverExpireConfig.CSV_HEADERS, mode="w")

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
        if "retentionInDays" in lg:
            return False

        log_group = lg["logGroupName"]
        stored_bytes = lg.get("storedBytes", 0)
        monthly_ingested_bytes = self._get_monthly_ingested_bytes(log_group)

        row = [
            log_group,
            round(stored_bytes / 1_000_000_000, 2),
            round(monthly_ingested_bytes / 1_000_000_000, 2),
        ]

        utils.write_to_csv(LogsNeverExpireConfig.OUTPUT_CSV, row, mode="a")
        return True

    def _sort_csv(self):
        df = pd.read_csv(LogsNeverExpireConfig.OUTPUT_CSV, encoding = "utf-8")
        df = df.sort_values(LogsNeverExpireConfig.SORT_BY_COLUMN, ascending = False)
        df.to_csv(LogsNeverExpireConfig.OUTPUT_CSV, index = False)

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Scanning CloudWatch Log Groups (Never Expire).")
        count = 0
        log_groups = list(self._fetch_log_groups())
        print(f"Processing {len(log_groups)} Log groups.")

        with ThreadPoolExecutor(max_workers=LogsNeverExpireConfig.MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_log_group, log_group) for log_group in log_groups]
            for future in as_completed(futures):
                if future.result():
                    count += 1

        self._sort_csv()
        print(f"Processed {count} log groups with never-expire retention.")
        print(f"Report written to {LogsNeverExpireConfig.OUTPUT_CSV}.")
