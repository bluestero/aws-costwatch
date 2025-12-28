from pipelines import (
    EC2IdlePipeline,
    EBSUnusedPipeline
)
from datetime import datetime

pipelines = [
    EC2IdlePipeline,
    # EBSUnusedPipeline,
]

for pipeline_cls in pipelines:
    pipeline_name = pipeline_cls.__name__
    print(f"[{datetime.now().isoformat()}] Starting pipeline: {pipeline_name}.")
    
    try:
        pipeline_obj = pipeline_cls()
        pipeline_obj.run()
        print(f"[{datetime.now().isoformat()}] Finished pipeline: {pipeline_name}.\n")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ERROR in pipeline {pipeline_name}: {e}.")
