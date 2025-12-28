from pathlib import Path

# -------------------------------------------
# Common Config
# -------------------------------------------
class CommonConfig:
    OUTPUT_CSV_DIR = Path(__file__).parent / "output_files"

# -------------------------------------------
# EBS Unused
# -------------------------------------------
class EBSUnusedConfig:
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ebs_unused.csv"
    CSV_HEADERS = ["ID", "Type", "Size (GB)", "Create Time", "Monthly Cost ($)"]

# -------------------------------------------
# EC2 Idle
# -------------------------------------------
class EC2IdleConfig:
    MAX_WORKERS = 8
    NUMBER_OF_DAYS = 14
    OUTPUT_CSV = CommonConfig.OUTPUT_CSV_DIR / "ec2_idle.csv"
    CSV_HEADERS = ["Instance ID", "Name", "Type", "Lifecycle", "State", "Launch Time", "Status", "Max CPU(%)","Max NetIn(MB)", "Max NetOut(MB)"]

