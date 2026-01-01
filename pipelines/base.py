import utils
import pandas as pd
from typing import Type
from utils import logger
from settings import CommonConfig
from concurrent.futures import ThreadPoolExecutor, as_completed


class BasePipeline:
    """
    Base class for all pipelines.

    Subclasses MUST define:
      - CONFIG
      - fetch_items()
      - process_item(item)
    """

    CONFIG: Type[CommonConfig]

    def __init__(self):
        self.pipeline_name = self.__class__.__name__
        utils.write_to_csv(self.CONFIG.OUTPUT_CSV, self.CONFIG.CSV_HEADERS, mode="w")

    def fetch_items(self):
        raise NotImplementedError

    def process_item(self, item) -> bool:
        raise NotImplementedError

    def post_process(self):

        # Sorting and saving the CSV.
        df = pd.read_csv(self.CONFIG.OUTPUT_CSV, encoding = "utf-8")
        df = df.sort_values(self.CONFIG.SORT_BY_COLUMN, ascending = self.CONFIG.SORT_ASCENDING)

        # Writing the DataFrame to the GSheet.
        if CommonConfig.WRITE_TO_GOOGLE_SHEET and not df.empty:
            utils.write_df_to_sheet(self.CONFIG.WORKSHEET_NAME, df)
            logger.info(f"[{self.pipeline_name}] Updated the {self.CONFIG.WORKSHEET_NAME} sheet successfully.")

    def run(self):
        items = self.fetch_items()
        logger.info(f"[{self.pipeline_name}] Processing {len(items)} items.")

        processed_count = 0

        with ThreadPoolExecutor(max_workers=self.CONFIG.MAX_WORKERS) as executor:
            futures = [executor.submit(self.process_item, item) for item in items]

            for future in as_completed(futures):
                if future.result():
                    processed_count += 1

        self.post_process()
        logger.info(f"[{self.pipeline_name}] Found {processed_count} relevant items.")
