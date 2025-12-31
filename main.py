from utils import logger
from pipelines import (
    EBSUnusedPipeline,
    EC2IdlePipeline,
    EC2UnusedPipeline,
    EIPUnusedPipeline,
    LogsNeverExpirePipeline,
    LogsHighIngestionPipeline,
    LambdaExcessMemoryPipeline,
    SnapshotOldPipeline,
    NATUnusedPipeline,
)

pipelines = [
    NATUnusedPipeline,
    # EBSUnusedPipeline,
    # EC2IdlePipeline,
    # EC2UnusedPipeline,
    # EIPUnusedPipeline,
    # LogsNeverExpirePipeline,
    # LogsHighIngestionPipeline,
    # LambdaExcessMemoryPipeline,
    # SnapshotOldPipeline,
]

for pipeline_cls in pipelines:
    pipeline_name = pipeline_cls.__name__
    logger.info(f"Starting pipeline: {pipeline_name}.")

    try:
        pipeline_obj = pipeline_cls()
        pipeline_obj.run()
        logger.info(f"Finished pipeline: {pipeline_name}.\n")
    except Exception as e:
        logger.exception(f"ERROR in pipeline: {pipeline_name}")
