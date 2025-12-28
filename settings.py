from pathlib import Path

# -------------------------------------------
# Common Config
# -------------------------------------------
class CommonConfig:
    OUTPUT_CSV_DIR = Path (__file__).parent / "output_files"

# -------------------------------------------
# EBS Unused
# -------------------------------------------
class EBSUnusedConfig:
    SORT_BY_COLUMN = "Size (GB)"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ebs_unused.csv"
    CSV_HEADERS = ["Volume ID", "Size (GB)", "Volume Type", "Created Time"]

# -------------------------------------------
# EC2 Idle
# -------------------------------------------
class EC2IdleConfig:
    MAX_WORKERS = 8
    NUMBER_OF_DAYS = 14
    SORT_BY_COLUMN = "Status"
    CPU_IDLE_THRESHOLD_PERCENTAGE = 5.0
    NET_IDLE_THRESHOLD_MB = 5 * 1024 * 1024
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ec2_idle.csv"
    CSV_HEADERS = ["Instance ID", "Name", "Type", "Lifecycle", "State", "Launch Time", "Status", "Max CPU (%)","Max NetIn (MB)", "Max NetOut (MB)"]

# -------------------------------------------
# EC2 Unused
# -------------------------------------------
class EC2UnusedConfig:
    MAX_WORKERS = 8
    SORT_BY_COLUMN = "EC2 Hourly Cost ($)"
    INPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ec2_idle.csv"
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ec2_unused.csv"
    CSV_HEADERS = ["Instance ID", "Name", "Type", "Lifecycle", "Status", "EC2 Hourly Cost ($)", "Volumes Attached", "Volume Size (GB)", "EIPs Attached"]

# -------------------------------------------
# EIP Unused
# -------------------------------------------
class EIPUnusedConfig:
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "eip_unused.csv"
    CSV_HEADERS = ["Public IP", "Allocation ID"]
