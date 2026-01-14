from datetime import datetime, timedelta, timezone

# ----------------------
# Custom Imports
# ----------------------
import utils
from utils import logger
from settings import DynamoDBUnusedConfig
from pipelines.base import BasePipeline


class DynamoDBUnusedPipeline(BasePipeline):
    CONFIG = DynamoDBUnusedConfig

    def __init__(self):
        super().__init__()

        # Clients
        session = utils.create_boto3_session()
        self.ddb = session.client("dynamodb")
        self.cw = session.client("cloudwatch")

        # Time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=self.CONFIG.LOOKBACK_DAYS)

    # ----------------------
    # Private helpers
    # ----------------------
    def _get_consumed_units(self, table_name: str, metric_name: str, index_name: str | None = None) -> float:
        dimensions = [{"Name": "TableName", "Value": table_name}]
        if index_name:
            dimensions.append({"Name": "GlobalSecondaryIndexName", "Value": index_name})

        resp = self.cw.get_metric_statistics(
            Namespace="AWS/DynamoDB",
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=self.start_time,
            EndTime=self.end_time,
            Period=86400,
            Statistics=["Sum"],
        )

        return sum(dp.get("Sum", 0) for dp in resp.get("Datapoints", []))

    def _get_avg_provisioned_units(self, table_name: str, metric_name: str, index_name: str | None = None) -> float:
        dimensions = [{"Name": "TableName", "Value": table_name}]
        if index_name:
            dimensions.append({"Name": "GlobalSecondaryIndexName", "Value": index_name})

        resp = self.cw.get_metric_statistics(
            Namespace="AWS/DynamoDB",
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=self.start_time,
            EndTime=self.end_time,
            Period=86400,
            Statistics=["Average"],
        )

        datapoints = resp.get("Datapoints", [])
        if not datapoints:
            return 0.0

        return sum(dp.get("Average", 0) for dp in datapoints) / len(datapoints)

    def _get_storage_and_item_counts(self, table_desc: dict) -> tuple[int, float]:
        table_items = table_desc.get("ItemCount", 0)
        table_size_gb = table_desc.get("TableSizeBytes", 0) / (1024 * 1024 * 1024)

        gsi_items = 0
        gsi_size_gb = 0.0

        for gsi in table_desc.get("GlobalSecondaryIndexes", []):
            gsi_items += gsi.get("ItemCount", 0)
            gsi_size_gb += gsi.get("IndexSizeBytes", 0) / (1024 * 1024 * 1024)

        return (table_items, gsi_items, table_size_gb, gsi_size_gb)

    def _get_gsi_list(self, table_desc: dict) -> list[dict]:
        return table_desc.get("GlobalSecondaryIndexes", [])

    def _get_provisioned_capacity(self, table_name: str, gsi_list: list[dict]) -> tuple[float, float]:
        provisioned_rcu = 0.0
        provisioned_wcu = 0.0

        provisioned_rcu += self._get_avg_provisioned_units(table_name, "ProvisionedReadCapacityUnits")
        provisioned_wcu += self._get_avg_provisioned_units(table_name, "ProvisionedWriteCapacityUnits")

        for gsi in gsi_list:
            index_name = gsi["IndexName"]
            provisioned_rcu += self._get_avg_provisioned_units(table_name, "ProvisionedReadCapacityUnits", index_name=index_name)
            provisioned_wcu += self._get_avg_provisioned_units(table_name, "ProvisionedWriteCapacityUnits", index_name=index_name)

        return provisioned_rcu, provisioned_wcu

    def _get_consumed_capacity(self, table_name: str, gsi_list: list[dict]) -> tuple[float, float]:
        total_read = self._get_consumed_units(table_name, "ConsumedReadCapacityUnits")
        total_write = self._get_consumed_units(table_name, "ConsumedWriteCapacityUnits")

        for gsi in gsi_list:
            index_name = gsi["IndexName"]
            total_read += self._get_consumed_units(table_name, "ConsumedReadCapacityUnits", index_name=index_name)
            total_write += self._get_consumed_units(table_name, "ConsumedWriteCapacityUnits", index_name=index_name)

        return total_read, total_write

    def _is_pitr_enabled(self, table_name: str) -> bool:
        resp = self.ddb.describe_continuous_backups(TableName=table_name)

        status = (
            resp.get("ContinuousBackupsDescription", {})
            .get("PointInTimeRecoveryDescription", {})
            .get("PointInTimeRecoveryStatus")
        )

        return status == "ENABLED"

    # -------------------------------
    # Required BasePipeline methods
    # -------------------------------
    def fetch_items(self):
        logger.info("Fetching DynamoDB tables.")
        paginator = self.ddb.get_paginator("list_tables")

        tables = []
        for page in paginator.paginate():
            tables.extend(page.get("TableNames", []))

        return tables

    def process_item(self, table_name: str) -> bool:
        desc = self.ddb.describe_table(TableName=table_name)["Table"]

        table_status = desc["TableStatus"]
        created_at = desc["CreationDateTime"]

        min_age = timedelta(days=self.CONFIG.LOOKBACK_DAYS + 1)
        if self.end_time - created_at < min_age:
            return False

        billing_mode = desc.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
        pitr_enabled = self._is_pitr_enabled(table_name)

        gsi_list = self._get_gsi_list(desc)
        gsi_count = len(gsi_list)

        table_items, gsi_items, table_size_gb, gsi_size_gb = self._get_storage_and_item_counts(desc)

        provisioned_rcu = provisioned_wcu = 0.0
        total_read_units = total_write_units = 0.0

        if table_status == "ACTIVE":
            total_read_units, total_write_units = self._get_consumed_capacity(table_name, gsi_list)

            if billing_mode == "PROVISIONED":
                provisioned_rcu, provisioned_wcu = self._get_provisioned_capacity(table_name, gsi_list)

        row = [
            table_name,
            billing_mode,
            round(table_items, 2),
            round(table_size_gb, 2),
            round(gsi_items, 2),
            round(gsi_size_gb, 2),
            round(provisioned_rcu, 2),
            round(provisioned_wcu, 2),
            round(total_read_units, 2),
            round(total_write_units, 2),
            created_at.strftime("%Y-%m-%d %H:%M:%S"),
            table_status,
            gsi_count,
            "YES" if pitr_enabled else "NO",
        ]

        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, row, mode="a")
        return True
