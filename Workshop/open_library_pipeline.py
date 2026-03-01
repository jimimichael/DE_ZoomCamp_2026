"""dlt pipeline for the Open Library REST API (books endpoint)."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from the Open Library REST API (books endpoint)."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
            # No auth required for reading; session/cookies only for write.
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    "method": "GET",
                    "params": {
                        "format": "json",
                        "jscmd": "data",
                        # Example bibkeys so the pipeline returns data; single request, no pagination.
                        "bibkeys": "ISBN:0451526538,ISBN:0201558025,ISBN:0385472579,ISBN:9780980200447",
                    },
                    "data_selector": "*",
                    "paginator": {"type": "single_page"},
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
