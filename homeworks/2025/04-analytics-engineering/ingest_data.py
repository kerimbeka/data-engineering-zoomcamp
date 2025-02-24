import polars as pl
from sqlalchemy import create_engine
from pathlib import Path
from tqdm import tqdm

def main():
    
    taxi_services = ["green","yellow", "fhv"]
    
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    
    for taxi_service in taxi_services:
        data_path = Path("./data") / taxi_service
        
        files = [f for f in data_path.iterdir() if f.is_file()]
        table_created = False
        
        for file in tqdm(files):
            df = pl.read_csv(file, n_threads=8, infer_schema_length=10000000) 
            if taxi_service=="green":
                df = df.with_columns(
                    pl.col("lpep_pickup_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
                    pl.col("lpep_dropoff_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                )
            elif taxi_service=="yellow":
                df = df.with_columns(
                    pl.col("tpep_pickup_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
                    pl.col("tpep_dropoff_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                )
            elif taxi_service=="fhv":
                df = df.with_columns(
                    pl.col("pickup_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
                    pl.col("dropOff_datetime").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                )
            
            if not table_created:
                df.write_database(
                    table_name=f"{taxi_service}_taxi_data",
                    connection=engine,
                    if_table_exists="replace"
                )
                table_created = True
            else:
                df.write_database(
                    table_name=f"{taxi_service}_taxi_data",
                    connection=engine,
                    if_table_exists="append"
                )

if __name__ == '__main__':
    main()