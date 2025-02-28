import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import logging

# Enable logging for debugging
logging.basicConfig(level=logging.INFO)

class CallAPI(beam.DoFn):
    def __init__(self, url, timeout):
        self.url = url
        self.timeout = timeout

    def process(self, element):
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
            logging.info(f"Success: {self.url} -> {response.text}")
            yield response.text  # Yield API response to the next step
        except requests.exceptions.RequestException as e:
            logging.error(f"Error calling API {self.url}: {str(e)}")
            yield f"Error: {self.url}"

def run():
    # Remove unnecessary GCP options for local execution
    pipeline_options = PipelineOptions(
        runner="DirectRunner",  # Ensure we're running locally
        save_main_session=True  # Required if using multiprocessing
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Start with a dummy element to trigger the pipeline
        start = p | "Start Pipeline" >> beam.Create([None])

        # Sequential API Calls (change URLs if needed)
        fileExtraction = start | "File Extraction" >> beam.ParDo(CallAPI(url="http://localhost:8081/func_file_extraction", timeout=30))
        erWithBlocking = fileExtraction | "ER with Blocking" >> beam.ParDo(CallAPI(url="http://localhost:8082/func_er_with_blocking", timeout=30))
        erWithBruteForce = erWithBlocking | "ER with Brute Force" >> beam.ParDo(CallAPI(url="http://localhost:8083/func_er_with_brute_force", timeout=100))
        qualityMeasures = erWithBruteForce | "Quality Measures" >> beam.ParDo(CallAPI(url="http://localhost:8084/func_calculate_quality_measures", timeout=30))
        erClustering = qualityMeasures | "ER Clustering" >> beam.ParDo(CallAPI(url="http://localhost:8085/func_er_clustering", timeout=30))

        # Output results
        erClustering | "Print Output" >> beam.Map(logging.info)
        # fileExtraction | "Print Output" >> beam.Map(logging.info)


if __name__ == "__main__":
    run()

# Run the pipeline
# command: python beamMain.py