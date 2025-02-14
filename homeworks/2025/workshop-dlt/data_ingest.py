import dlt
import duckdb

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

from typing import Any, Generator


@dlt.resource(name="rides")
def ny_taxi() -> Generator[Any, Any, Any]:
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(base_page=1, total_path=None),
    )
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page


def main() -> None:
    print("dlt version:", dlt.__version__)

    pipeline = dlt.pipeline(
        pipeline_name="ny_taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data",
    )

    load_info = pipeline.run(ny_taxi)
    print(load_info)

    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

    conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

    # List all tables in the current schema
    tables = conn.sql("SHOW TABLES").df()

    # Print the number of tables
    print(f"Number of tables: {len(tables)}")

    df = pipeline.dataset(dataset_type="default").rides.df()
    print(f"The total number of records: {len(df)}")

    with pipeline.sql_client() as client:
        res = client.execute_sql(
            """
                SELECT
                AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
                FROM rides;
                """
        )
        # Prints column values of the first row
        print(res)


if __name__ == "__main__":
    main()
