from pathlib import Path


# -------------------------------------------
# Common Config
# -------------------------------------------
OUTPUT_CSV_DIR = Path(__file__).parent / "output_files"


# -------------------------------------------
# EBS Unused
# -------------------------------------------
EBS_OUTPUT_CSV = OUTPUT_CSV_DIR / "ebs_unused.csv"
EBS_CSV_HEADERS = ["ID", "Type", "Size (GB)", "Create Time", "Monthly Cost ($)"]
