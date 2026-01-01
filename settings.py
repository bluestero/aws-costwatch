from pathlib import Path


# -------------------------------------------
# Common Config
# -------------------------------------------
class CommonConfig:

    # Basic
    MAX_CPU_WORKERS = 4
    AWS_REGION = "us-east-1"
    MAIN_DIR = Path(__file__).parent
    OUTPUT_CSV_DIR = MAIN_DIR / "output_files"

    # Google Sheet
    WRITE_TO_GOOGLE_SHEET = True
    SPREADSHEET_NAME = "AWS Cost Watch - Onclusive Management"
    GCC_JSON_PATH = MAIN_DIR / "google-sheet-creds.json"

# -------------------------------------------
# EBS Unused
# -------------------------------------------
class EBSUnusedConfig(CommonConfig):
    MAX_WORKERS = 8
    LOOKBACK_DAYS = 14
    SORT_BY_COLUMN = "Size (GB)"
    WORKSHEET_NAME = "EBS - Unused"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ebs_unused.csv"
    CSV_HEADERS = ["Volume ID", "Size (GB)", "Volume Type", "Created Time"]

# -------------------------------------------
# EC2 Idle
# -------------------------------------------
class EC2IdleConfig(CommonConfig):
    MAX_WORKERS = 8
    LOOKBACK_DAYS = 14
    SORT_BY_COLUMN = "Status"
    CPU_IDLE_THRESHOLD_PERCENTAGE = 5.0
    NET_IDLE_THRESHOLD_MB = 5 * 1024 * 1024
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ec2_idle.csv"
    CSV_HEADERS = ["Instance ID", "Name", "Type", "Lifecycle", "State", "Launch Time", "Status", "Max CPU (%)","Max NetIn (MB)", "Max NetOut (MB)"]

# -------------------------------------------
# EC2 Unused
# -------------------------------------------
class EC2UnusedConfig(CommonConfig):
    MAX_WORKERS =  8
    WORKSHEET_NAME = "EC2 - Unused"
    SORT_BY_COLUMN = "EC2 Hourly Cost ($)"
    INPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ec2_idle.csv"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ec2_unused.csv"
    CSV_HEADERS = ["Instance ID", "Name", "Type", "Lifecycle", "Status", "EC2 Hourly Cost ($)", "Volumes Attached", "Volume Size (GB)", "EIPs Attached"]

# -------------------------------------------
# EIP Unused
# -------------------------------------------
class EIPUnusedConfig(CommonConfig):
    SORT_BY_COLUMN = "Public IP"
    WORKSHEET_NAME = "EIP - Unused"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "eip_unused.csv"
    CSV_HEADERS = ["Public IP", "Allocation ID"]

# -------------------------------------------
# Logs Never Expire
# -------------------------------------------
class LogsNeverExpireConfig(CommonConfig):
    MAX_WORKERS = 6
    SORT_BY_COLUMN = "Stored (GB)"
    WORKSHEET_NAME = "Logs - Never Expire"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "logs_never_expire.csv"
    CSV_HEADERS = ["Log Group", "Stored (GB)", "Monthly Ingested (GB)"]

# -------------------------------------------
# Logs High Ingestion
# -------------------------------------------
class LogsHighIngestionConfig(CommonConfig):
    MAX_WORKERS = 6
    INGESTION_THRESHOLD_GB = 1000
    SORT_BY_COLUMN = "Monthly Ingested (GB)"
    CSV_HEADERS = ["Log Group", "Monthly Ingested (GB)"]
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "logs_high_ingestion.csv"

# -------------------------------------------
# Lambda Excess Memory
# -------------------------------------------
class LambdaExcessMemoryConfig(CommonConfig):
    MAX_WORKERS =  8
    INVOCATION_LOOKBACK_DAYS = 30
    SORT_BY_COLUMN = "Invocations"
    LOGS_INSIGHTS_LOOKBACK_DAYS = 7
    WORKSHEET_NAME = "Lambda - Excess Memory"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "lambda_excess_memory.csv"
    CSV_HEADERS = ["Lambda Name", "Assigned Memory (MB)", "Invocations", "Avg Bill Duration (seconds)", "Avg Memory Used", "Max Memory Used"]

# -------------------------------------------
# Snapshot Old
# -------------------------------------------
class SnapshotOldConfig(CommonConfig):
    MAX_WORKERS =  8
    SORT_BY_COLUMN = "Size (GB)"
    WORKSHEET_NAME = "Snapshot - Old"
    SNAPSHOT_CUTOFF_DATE = "2024-05-01"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "snapshot_old.csv"
    CSV_HEADERS = ["Snapshot ID", "Volume ID", "Volume Name", "Volume Type", "Attached Instance ID", "Attached Instance Name", "Size (GB)", "Snapshot Date"]

# -------------------------------------------
# NAT Gateway Unused
# -------------------------------------------
class NATUnusedConfig(CommonConfig):
    MAX_WORKERS = 8
    LOOKBACK_DAYS = 30
    WORKSHEET_NAME = "NAT - Unused"
    SORT_BY_COLUMN = "Created Time"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "nat_unused.csv"
    CSV_HEADERS = ["NAT Gateway ID", "Vpc ID", "State", "Subnet ID", "Created Time"]
