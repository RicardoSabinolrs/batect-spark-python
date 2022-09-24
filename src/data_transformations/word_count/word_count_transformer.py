import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, translate


def word_count(word_df):
    """Creates a DataFrame with word counts.

      Args:
          word_df (DataFrame of str): A DataFrame consisting of one string column called 'word'.

      Returns:
          DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
      """
    return (word_df.withColumn('word', explode(split(translate(col('value'), r"'\"-_~^,.:;\\|/\ ", ""), ' ')))
            .groupBy('word')
            .count()
            .sort('count', ascending=False))


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    output_df = word_count(input_df)

    logging.info("Writing csv to directory: %s", output_path)

    output_df.coalesce(1).write.csv(output_path, header=True)
