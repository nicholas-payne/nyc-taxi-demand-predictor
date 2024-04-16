import wget
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc,expr,unix_timestamp,extract,lit
import itertools as it

def scrape_data():

    ''' 
    Downloads the raw taxi ride data from the NYC Taxi and Limousine Commission as parquet files 
    The data is stored in parquet format to reduce filesize. The full dataset is close to 300GB in size
    so the script may take a while to run
    '''
    # Full Range of Data Here #
    #years = range(2009,2024)
    months = range(1,13)
    ###########################

    # Limited Sample of Data for Testing #
    years = [2013]
    #months = [3]
    ######################################

    ym_list = list(it.product(years,months))
    
    while True:        
        folder = input("Enter path to download files: ")
        
        if os.path.exists(folder):
            print(f'Files will be downloaded to {folder}')
            
            approval = input('y/n? ')
            
            if approval == 'y':
                print('Commencing download')
                break

        else:
           print(f'{filepath} is an invalid file path')

    for ym in ym_list:
         url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym[0]}-{ym[1]:02d}.parquet"
         filename = f"yellow_tripdata_{ym[0]}-{ym[1]:02d}.parquet"
         filepath = folder + '/' + filename
         
         wget.download(url, filepath)
                  
         spark = SparkSession.builder \
            .master('local') \
            .appName('myAppName') \
            .config('spark.executor.memory', '5gb') \
            .config("spark.cores.max", "6") \
            .getOrCreate()
         
         df = spark.read.parquet(filepath)

         df_filtered = df \
            .select(
                date_trunc('hour', df.tpep_pickup_datetime).alias('pickup_hour'),
                "PULocationID",
                "trip_distance",
                "fare_amount",
                "payment_type"
                ) \
            .filter(
                (extract(lit('YEAR'),df.tpep_pickup_datetime) == ym[0]) &
                ((unix_timestamp(df.tpep_dropoff_datetime) - unix_timestamp(df.tpep_pickup_datetime))/60 > 0) &
                (df.RatecodeID != '5.0') &
                (df.trip_distance > 0) &
                (df.payment_type.isin(['1','2']))
                )

         df_agg = df_filtered \
             .groupBy("pickup_hour","PULocationID") \
             .agg({"trip_distance": "avg","fare_amount":"avg","payment_type":"count"}) \
             .orderBy(["pickup_hour","PULocationID"])

         df_agg_pandas = df_agg.toPandas()
         csv_filepath = path + '/' + 'agg_taxi_data_{ym[0]}_{ym[1]:02d}.csv'
         df_agg_pandas.to_csv(csv_filepath)

         os.remove(filepath)

if __name__ == '__main__':
    scrape_data()