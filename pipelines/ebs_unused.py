import utils
import settings


class UnusedEBSReporter:
    EBS_COST_PER_GB = 0.08

    def __init__(self):
        self.total_cost = 0
        session = utils.create_boto3_session()
        self.ec2 = session.client("ec2")
        utils.write_to_csv(settings.EBS_OUTPUT_CSV, settings.EBS_CSV_HEADERS, mode="w")


    def _fetch_unused_volumes(self):
        filter = [{"Name": "status", "Values": ["available"]}]
        response = self.ec2.describe_volumes(Filters=filter)
        return response.get("Volumes", [])


    def _process_volume(self, volume: dict):

        size = volume["Size"]
        vol_id = volume["VolumeId"]
        vtype = volume["VolumeType"]
        create_time = volume["CreateTime"].strftime("%Y-%m-%d %H:%M:%S") if hasattr(volume["CreateTime"], "strftime") else volume["CreateTime"]

        monthly_cost = size * self.EBS_COST_PER_GB
        self.total_cost += monthly_cost

        row = [vol_id, vtype, size, create_time, monthly_cost]
        utils.write_to_csv(settings.EBS_OUTPUT_CSV, row, mode="a")


    def run(self):
        volumes = self._fetch_unused_volumes()
        for volume in volumes:
            self._process_volume(volume)        
        print(f"Total monthly cost of unused EBS volumes: ${self.total_cost:.2f}")

# -------------------------------------------
# Run the reporter
# -------------------------------------------
if __name__ == "__main__":
    reporter = UnusedEBSReporter()
    reporter.run()
