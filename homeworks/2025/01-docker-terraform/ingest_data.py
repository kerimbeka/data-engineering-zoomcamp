import polars as pl
from sqlalchemy import create_engine

def main():
    
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    
    reader = pl.read_csv_batched("data/green_tripdata_2019-10.csv.gz") 
    batches = reader.next_batches(250)
    
    for i, df in enumerate(batches):
        df = df.with_columns(
            pl.col("lpep_pickup_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.col("lpep_dropoff_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
        )
        if i == 0:
            df.write_database(
                table_name="green_taxi_data",
                connection=engine,
                if_table_exists="replace"
            )
        else:
            df.write_database(
                table_name="green_taxi_data",
                connection=engine,
                if_table_exists="append"
            )
    
    df_zones = pl.read_csv('data/taxi_zone_lookup.csv')
    df_zones.write_database(table_name='zones', connection=engine, if_table_exists='replace')

if __name__ == '__main__':
    main()