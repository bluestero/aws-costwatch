import time
from datetime import datetime, timedelta, timezone

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import LambdaExcessMemoryConfig
from pipelines.base import BasePipeline


class LambdaExcessMemoryPipeline(BasePipeline):
    CONFIG = LambdaExcessMemoryConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.lambda_client = session.client("lambda")
        self.cw = session.client("cloudwatch")
        self.logs = session.client("logs")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.invocation_start_time = self.end_time - timedelta(days=self.CONFIG.INVOCATION_LOOKBACK_DAYS)
        self.logs_insights_start_time = self.end_time - timedelta(days=self.CONFIG.LOGS_INSIGHTS_LOOKBACK_DAYS)

    # ----------------------
    # Private helpers
    # ----------------------
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

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Fetching Lambda functions.")
        paginator = self.lambda_client.get_paginator("list_functions")

        lambdas = []
        for page in paginator.paginate():
            for fn in page.get("Functions", []):
                lambdas.append(
                    {
                        "name": fn["FunctionName"],
                        "memory": fn["MemorySize"],
                    }
                )

        return lambdas

    def process_item(self, fn: dict) -> bool:
        name = fn["name"]
        memory = fn["memory"]

        try:
            invocations = self._get_invocations(name)

            logs_metrics = self._get_logs_metrics(f"/aws/lambda/{name}")
            if not logs_metrics:
                logs_metrics = self._get_logs_metrics(f"/lambda/{name}")

            avg_billed_seconds = round(logs_metrics.get("avg_billed", 0) / 1000, 2)
            avg_memory = int(logs_metrics.get("avg_memory", 0))
            max_memory = int(logs_metrics.get("max_memory", 0))

            row = [name, memory, invocations, avg_billed_seconds, avg_memory, max_memory]

            utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
            return True

        except Exception as e:
            logger.info(f"{self.pipeline_name}: Failed {name}: {e}.")
            return False
