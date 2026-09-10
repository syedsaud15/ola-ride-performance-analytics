"""Databricks/PySpark pipeline for ride performance analytics."""
from __future__ import annotations

import argparse
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

TRIP_SCHEMA = T.StructType([
    T.StructField("trip_id", T.StringType(), False),
    T.StructField("date", T.DateType(), False),
    T.StructField("city_id", T.StringType(), False),
    T.StructField("passenger_type", T.StringType(), True),
    T.StructField("distance_travelled_km", T.DoubleType(), True),
    T.StructField("fare_amount", T.DoubleType(), True),
    T.StructField("passenger_rating", T.IntegerType(), True),
    T.StructField("driver_rating", T.IntegerType(), True),
])

CITY_SCHEMA = T.StructType([
    T.StructField("city_id", T.StringType(), False),
    T.StructField("city_name", T.StringType(), False),
])


def read_bronze_trips(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True).schema(TRIP_SCHEMA).csv(path)
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingested_at", F.current_timestamp())
    )


def clean_trips(bronze: DataFrame) -> DataFrame:
    valid = (
        bronze
        .withColumn("passenger_category", F.lower(F.trim("passenger_type")))
        .withColumnRenamed("date", "business_date")
        .withColumnRenamed("distance_travelled_km", "distance_kms")
        .withColumnRenamed("fare_amount", "sales_amt")
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("business_date").isNotNull())
        .filter(F.col("city_id").isNotNull())
        .filter(F.col("distance_kms") >= 0)
        .filter(F.col("sales_amt") >= 0)
        .filter(F.col("passenger_rating").between(1, 10))
        .filter(F.col("driver_rating").between(1, 10))
    )
    newest = Window.partitionBy("trip_id").orderBy(F.col("ingested_at").desc())
    return valid.withColumn("row_number", F.row_number().over(newest)).filter("row_number = 1").drop("row_number")


def build_calendar(spark: SparkSession, start: date, end: date) -> DataFrame:
    calendar = spark.sql(
        f"select explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) as business_date"
    )
    return (
        calendar
        .withColumn("year", F.year("business_date"))
        .withColumn("month", F.month("business_date"))
        .withColumn("month_name", F.date_format("business_date", "MMMM"))
        .withColumn("quarter", F.quarter("business_date"))
        .withColumn("week_of_year", F.weekofyear("business_date"))
        .withColumn("day_of_week", F.date_format("business_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("business_date").isin(1, 7))
        .withColumn("is_weekday", ~F.col("is_weekend"))
        .withColumn(
            "national_holiday",
            ((F.month("business_date") == 1) & (F.dayofmonth("business_date") == 26))
            | ((F.month("business_date") == 8) & (F.dayofmonth("business_date") == 15))
            | ((F.month("business_date") == 10) & (F.dayofmonth("business_date") == 2)),
        )
    )


def build_gold(trips: DataFrame, cities: DataFrame, calendar: DataFrame) -> DataFrame:
    return (
        trips.join(F.broadcast(cities), "city_id", "inner")
        .join(calendar, "business_date", "inner")
        .select(
            "trip_id", "business_date", "city_id", "city_name", "passenger_category",
            "distance_kms", "sales_amt", "passenger_rating", "driver_rating",
            "year", "month", "month_name", "quarter", "week_of_year", "day_of_week",
            "is_weekday", "is_weekend", "national_holiday", "ingested_at", "source_file",
        )
    )


def publish(gold: DataFrame, table_name: str, mode: str) -> None:
    if mode == "full" or not gold.sparkSession.catalog.tableExists(table_name):
        gold.write.format("delta").mode("overwrite").partitionBy("year", "month").saveAsTable(table_name)
        return
    target = DeltaTable.forName(gold.sparkSession, table_name)
    (
        target.alias("target")
        .merge(gold.alias("source"), "target.trip_id = source.trip_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("ola-ride-performance").getOrCreate()
    spark.sql(f"create catalog if not exists {args.catalog}")
    spark.sql(f"create schema if not exists {args.catalog}.gold")
    bronze = read_bronze_trips(spark, args.trips_path)
    silver = clean_trips(bronze)
    cities = spark.read.option("header", True).schema(CITY_SCHEMA).csv(args.cities_path).dropDuplicates(["city_id"])
    bounds = silver.agg(F.min("business_date").alias("start"), F.max("business_date").alias("end")).first()
    if not bounds.start or not bounds.end:
        raise ValueError("No valid trips remain after validation.")
    gold = build_gold(silver, cities, build_calendar(spark, bounds.start, bounds.end))
    publish(gold, f"{args.catalog}.gold.fact_trips", args.mode)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trips-path", required=True)
    parser.add_argument("--cities-path", required=True)
    parser.add_argument("--catalog", default="transportation")
    parser.add_argument("--mode", choices=("full", "incremental"), default="full")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())

