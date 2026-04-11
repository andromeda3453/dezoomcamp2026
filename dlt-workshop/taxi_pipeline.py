import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from typing import Any

@dlt.source(name="taxi_data")
def taxi_source(
    base_url: str = dlt.config.value,
) -> Any:
    """
    Source for loading taxi transaction data from the Zoomcamp API.
    
    Args:
        base_url: The base URL of the API. Auto-loaded from config.toml.
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
        },
        "resources": [
            {
                "name": "taxi_trips",
                "endpoint": {
                    "path": "",  # The base URL itself is the endpoint
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        "total_path": None, # Stop when empty page is returned
                    },
                },
            }
        ],
    }
    yield from rest_api_resources(config)

def load_taxi_data() -> None:
    # Initialize the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination='duckdb',
        dataset_name="taxi_data",
        dev_mode=True,
    )

    # Run the pipeline
    load_info = pipeline.run(taxi_source())
    print(load_info)

if __name__ == "__main__":
    load_taxi_data()
