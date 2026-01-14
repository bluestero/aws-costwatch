import pipelines
from utils import logger
from settings import CommonConfig
from concurrent.futures import ProcessPoolExecutor, as_completed

pipelines_to_run = [
    pipelines.NATUnusedPipeline,
    pipelines.EBSUnusedPipeline,
    pipelines.EC2UnusedPipeline,
    pipelines.EIPUnusedPipeline,
    pipelines.SnapshotOldPipeline,
    pipelines.DynamoDBUnusedPipeline,
    pipelines.LogsNeverExpirePipeline,
    pipelines.LogsHighIngestionPipeline,
    pipelines.LambdaExcessMemoryPipeline,
    pipelines.KinesisExcessShardsPipeline,
]

def run_pipeline(pipeline_cls):
    pipeline_name = pipeline_cls.__name__
    logger.info(f"Starting pipeline: {pipeline_name}.")
    pipeline = pipeline_cls()
    pipeline.run()
    logger.info(f"Finished pipeline: {pipeline_name}.")
    return pipeline_name


if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=CommonConfig.MAX_CPU_WORKERS) as executor:
        futures = {executor.submit(run_pipeline, pipeline_cls): pipeline_cls for pipeline_cls in pipelines_to_run}
        for future in as_completed(futures):
            pipeline_cls = futures[future]
            pipeline_name = pipeline_cls.__name__
            try:
                future.result()
            except Exception:
                logger.exception(f"ERROR in pipeline: {pipeline_name}.")
