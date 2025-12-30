import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------
# Custom Imports
# ----------------------
import utils
from settings import LambdaExcessMemoryConfig


class LambdaExcessMemoryPipeline:

    def __init__(self):

        # Clients
        session = utils.create_boto3_session()
        self.lambda_client = session.client("lambda")
        self.cw = session.client("cloudwatch")
        self.logs = session.client("logs")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.invocation_start_time = self.end_time - timedelta(days=LambdaExcessMemoryConfig.INVOCATION_LOOKBACK_DAYS)
        self.logs_insights_start_time = self.end_time - timedelta(days=LambdaExcessMemoryConfig.LOGS_INSIGHTS_LOOKBACK_DAYS)

        # Writing headers
        utils.write_to_csv(LambdaExcessMemoryConfig.OUTPUT_CSV, LambdaExcessMemoryConfig.CSV_HEADERS, mode="w")

    # ----------------------
    # Private helpers
    # ----------------------
    def _fetch_lambdas(self):
        paginator = self.lambda_client.get_paginator("list_functions")
        for page in paginator.paginate():
            for fn in page.get("Functions", []):
                yield {
                    "name": fn["FunctionName"],
                    "memory": fn["MemorySize"],
                }

    def _get_invocations(self, function_name: str) -> int:
        resp = self.cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": function_name}],
            StartTime=self.invocation_start_time,
            EndTime=self.end_time,
            Period=3600,
            Statistics=["Sum"],
        )
        return int(sum(dp["Sum"] for dp in resp.get("Datapoints", [])))

    def _get_logs_metrics(self, log_group: str) -> dict:
        query = """
        filter @message like /REPORT RequestId/
        | parse @message /Billed Duration: (?<billed>[0-9.]+) ms/
        | parse @message /Max Memory Used: (?<memory>[0-9.]+) MB/
        | stats
            avg(billed) as avg_billed,
            avg(memory) as avg_memory,
            max(memory) as max_memory
        """

        try:
            resp = self.logs.start_query(
                logGroupName=log_group,
                startTime=int(self.logs_insights_start_time.timestamp()),
                endTime=int(self.end_time.timestamp()),
                queryString=query,
            )
        except self.logs.exceptions.ResourceNotFoundException:
            return {}

        query_id = resp["queryId"]

        while True:
            result = self.logs.get_query_results(queryId=query_id)
            if result["status"] == "Complete":
                break
            time.sleep(1)

        if not result.get("results"):
            return {}

        return {item["field"]: float(item["value"]) for item in result["results"][0]}

    def _process_lambda(self, fn: dict):
        name = fn["name"]
        memory = fn["memory"]

        invocations = self._get_invocations(name)
        logs_metrics = self._get_logs_metrics(f"/aws/lambda/{name}")
        if not logs_metrics:
            logs_metrics = self._get_logs_metrics(f"/lambda/{name}")

        avg_billed_seconds = round(logs_metrics.get("avg_billed", 0) / 1000, 2)
        avg_memory = int(logs_metrics.get("avg_memory", 0))
        max_memory = int(logs_metrics.get("max_memory", 0))

        row = [
            name,
            memory,
            invocations,
            avg_billed_seconds,
            avg_memory,
            max_memory,
        ]

        utils.write_to_csv(LambdaExcessMemoryConfig.OUTPUT_CSV, row, mode="a")
        return True

    def _safe_process_lambda(self, fn):
        try:
            return self._process_lambda(fn)
        except Exception as e:
            print(f"Failed {fn['name']}: {e}")
            return False

    def _sort_csv(self):
        df = pd.read_csv(LambdaExcessMemoryConfig.OUTPUT_CSV, encoding = "utf-8")
        df = df.sort_values(LambdaExcessMemoryConfig.SORT_BY_COLUMN, ascending = False)
        df.to_csv(LambdaExcessMemoryConfig.OUTPUT_CSV, index = False)

    # ----------------------
    # Main run function
    # ----------------------
    def run(self):
        print("Fetching Lambda functions.")
        count = 0
        lambdas = list(self._fetch_lambdas())
        print(f"Processing {len(lambdas)} Lambda functions.")

        with ThreadPoolExecutor(max_workers=LambdaExcessMemoryConfig.MAX_WORKERS) as executor:
            futures = [executor.submit(self._safe_process_lambda, fn) for fn in lambdas]
            for future in as_completed(futures):
                if future.result():
                    count += 1

        self._sort_csv()
        print(f"Finished processing the Lambdas. Failed: {len(lambdas) - count}.")
        print(f"Report written to {LambdaExcessMemoryConfig.OUTPUT_CSV}.")
