import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    return (dataframe.withColumn("distance", (
            f.pow(f.sin(f.radians(f.col('end_station_latitude') - f.col('start_station_latitude')) / 2), 2) +
            f.cos(f.radians(f.col('start_station_latitude'))) * f.cos(f.radians(f.col('end_station_latitude'))) *
            f.pow(f.sin(f.radians(f.col('end_station_longitude') - f.col('start_station_longitude')) / 2), 2)
    )).withColumn("distance", f.round(f.atan2(f.sqrt(f.col("distance")), f.sqrt(-f.col("distance") + 1)) * 12742000 / METERS_PER_MILE,2))).sort(f.col("tripduration"))


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
