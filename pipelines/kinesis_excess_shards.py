from typing import Any
from datetime import datetime, timedelta, timezone

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from pipelines.base import BasePipeline
from settings import KinesisExcessShardsConfig


class KinesisExcessShardsPipeline(BasePipeline):
    CONFIG = KinesisExcessShardsConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.kinesis = session.client("kinesis")
        self.cw = session.client("cloudwatch")

        # Time range
        self.period_seconds = 12 * 60 * 60
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.CONFIG.LOOKBACK_DAYS)

    # ----------------------
    # Private helpers
    # ----------------------
    def _list_stream_names(self) -> list[str]:
        paginator = self.kinesis.get_paginator("list_streams")
        streams: list[str] = []
        for page in paginator.paginate():
            streams.extend(page.get("StreamNames", []))
        return streams

    def _describe_stream_summary(self, stream_name: str) -> dict[str, Any]:
        resp = self.kinesis.describe_stream_summary(StreamName=stream_name)
        return resp["StreamDescriptionSummary"]

    def _get_stream_mode(self, summary: dict[str, Any]) -> str:
        mode = (summary.get("StreamModeDetails", {}).get("StreamMode", "PROVISIONED"))
        return mode

    def _get_retention_hours(self, summary: dict[str, Any]) -> int:
        return int(summary.get("RetentionPeriodHours", 24))

    def _get_provisioned_open_shard_count(self, summary: dict[str, Any], mode: str) -> int:
        if mode != "PROVISIONED":
            return 0
        return int(summary.get("OpenShardCount", 0))

    def _get_consumer_count(self, stream_arn: str) -> int:
        paginator = self.kinesis.get_paginator("list_stream_consumers")
        count = 0
        for page in paginator.paginate(StreamARN=stream_arn):
            count += len(page.get("Consumers", []))
        return count

    def _get_stream_level_metric_series(self, stream_name: str) -> dict[str, list[float]]:
        """
        Monthly lookback, 12-hour datapoints.
        - Read bytes: OutgoingBytes
        - Write bytes: IncomingBytes
        - Iterator age: GetRecords.IteratorAgeMilliseconds
        """
        resp = self.cw.get_metric_data(
            MetricDataQueries=[
                {
                    "Id": "incoming",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Kinesis",
                            "MetricName": "IncomingBytes",
                            "Dimensions": [{"Name": "StreamName", "Value": stream_name}],
                        },
                        "Period": self.period_seconds,
                        "Stat": "Sum",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "outgoing",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Kinesis",
                            "MetricName": "OutgoingBytes",
                            "Dimensions": [{"Name": "StreamName", "Value": stream_name}],
                        },
                        "Period": self.period_seconds,
                        "Stat": "Sum",
                    },
                    "ReturnData": True,
                },
                {
                    "Id": "iterator_age",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/Kinesis",
                            "MetricName": "GetRecords.IteratorAgeMilliseconds",
                            "Dimensions": [{"Name": "StreamName", "Value": stream_name}],
                        },
                        "Period": self.period_seconds,
                        "Stat": "Maximum",
                    },
                    "ReturnData": True,
                },
            ],
            StartTime=self.start_time,
            EndTime=self.end_time,
            ScanBy="TimestampAscending",
        )

        results = {r["Id"]: r.get("Values", []) for r in resp.get("MetricDataResults", [])}
        return {
            "incoming_bytes": results.get("incoming", []),
            "outgoing_bytes": results.get("outgoing", []),
            "iterator_age_ms": results.get("iterator_age", []),
        }

    def _bytes_to_avg_mb_per_sec(self, datapoints_sum_bytes: list[float]) -> float:
        if not datapoints_sum_bytes:
            return 0.0

        mbps_values = [(b / (1024 * 1024)) / self.period_seconds for b in datapoints_sum_bytes]
        return sum(mbps_values) / len(mbps_values)

    def _bytes_to_max_mb_per_sec(self, datapoints_sum_bytes: list[float]) -> float:
        if not datapoints_sum_bytes:
            return 0.0
        max_bytes = max(datapoints_sum_bytes)
        return (max_bytes / (1024 * 1024)) / self.period_seconds

    def _bytes_to_total_gb(self, datapoints_sum_bytes: list[float]) -> float:
        if not datapoints_sum_bytes:
            return 0.0
        return sum(datapoints_sum_bytes) / (1024 ** 3)

    def _max_iterator_age_seconds(self, iterator_age_ms_points: list[float]) -> float:
        if not iterator_age_ms_points:
            return 0.0
        return max(iterator_age_ms_points) / 1000.0

    def _classify_traffic_pattern(self, avg_write_mbps: float, max_write_mbps: float,
                                  avg_read_mbps: float, max_read_mbps: float) -> str:
        """
        Simple, explainable classifier:
        - IDLE: basically no traffic
        - CONSISTENT: spikes not much higher than average
        - SPIKY: max >> avg (burst workloads)
        """
        avg = max(avg_write_mbps, avg_read_mbps)
        peak = max(max_write_mbps, max_read_mbps)

        if peak < 0.01 and avg < 0.005:
            return "IDLE"

        # Ratio-based spike detection
        if avg > 0 and (peak / avg) >= 5:
            return "SPIKY"

        return "CONSISTENT"

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Fetching Kinesis streams.")
        return self._list_stream_names()

    def process_item(self, stream_name: str) -> bool:
        summary = self._describe_stream_summary(stream_name)

        mode = self._get_stream_mode(summary)
        retention_hours = self._get_retention_hours(summary)
        shard_count = self._get_provisioned_open_shard_count(summary, mode)

        stream_arn = summary.get("StreamARN", "")
        consumer_count = self._get_consumer_count(stream_arn) if stream_arn else 0

        metric_series = self._get_stream_level_metric_series(stream_name)

        # Read = outgoing, Write = incoming
        incoming_bytes = metric_series["incoming_bytes"]
        outgoing_bytes = metric_series["outgoing_bytes"]
        iterator_age_ms = metric_series["iterator_age_ms"]

        avg_write_mbps = self._bytes_to_avg_mb_per_sec(incoming_bytes)
        avg_read_mbps = self._bytes_to_avg_mb_per_sec(outgoing_bytes)

        max_write_mbps = self._bytes_to_max_mb_per_sec(incoming_bytes)
        max_read_mbps = self._bytes_to_max_mb_per_sec(outgoing_bytes)

        total_monthly_write_gb = self._bytes_to_total_gb(incoming_bytes)
        total_monthly_read_gb = self._bytes_to_total_gb(outgoing_bytes)

        max_iterator_age_sec = self._max_iterator_age_seconds(iterator_age_ms)

        traffic_pattern = self._classify_traffic_pattern(
            avg_write_mbps=avg_write_mbps,
            max_write_mbps=max_write_mbps,
            avg_read_mbps=avg_read_mbps,
            max_read_mbps=max_read_mbps,
        )

        row = [
            stream_name,
            mode,
            traffic_pattern,
            shard_count,
            retention_hours,
            consumer_count,
            round(avg_read_mbps, 4),
            round(avg_write_mbps, 4),
            round(max_read_mbps, 4),
            round(max_write_mbps, 4),
            round(total_monthly_read_gb, 2),
            round(total_monthly_write_gb, 2),
            round(max_iterator_age_sec, 2),
        ]

        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True
