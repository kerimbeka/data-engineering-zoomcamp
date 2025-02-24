1. select * from myproject.my_nyc_tripdata.ext_green_taxi
2. Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
3. dbt run --select models/staging/+
4. 
   1. Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile
   2. When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
   3. When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
   4. When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET

5. green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
6. green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
7. LaGuardia Airport, Chinatown, Garment District

dbt steps:
1. uv run dbt deps 
2. uv run dbt seed
3. make all columns lowercase
4. uv run dbt run --select path_to_model
