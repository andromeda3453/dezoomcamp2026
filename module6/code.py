import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import pyspark
    from pyspark.sql import SparkSession

    return (SparkSession,)


@app.cell
def _(SparkSession):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
    return (spark,)


@app.cell
def _():
    import pandas as pd

    return (pd,)


@app.cell
def _(pd):
    df_pandas = pd.read_csv("head.csv")
    return (df_pandas,)


@app.cell
def _(df_pandas):
    df_pandas.dtypes
    return


@app.cell
def _(df_pandas, spark):
    spark.createDataFrame(df_pandas).schema
    return


@app.cell
def _():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

    return IntegerType, StringType, StructField, StructType, TimestampType


@app.cell
def _(IntegerType, StringType, StructField, StructType, TimestampType):
    schema = StructType([StructField('hvfhs_license_num', StringType(), True), StructField('dispatching_base_num', StringType(), True), StructField('pickup_datetime', TimestampType(), True), StructField('dropoff_datetime', TimestampType(), True), StructField('PULocationID', IntegerType(), True), StructField('DOLocationID', IntegerType(), True), StructField('SR_Flag', StringType(), True)])
    return (schema,)


@app.cell
def _(schema, spark):
    df = spark.read \
        .option("header", "true") \
        .schema(schema=schema) \
        .csv("fhvhv_tripdata_2021-01.csv")
    return (df,)


@app.cell
def _(df):
    df.head(10)
    return


@app.cell
def _(spark):
    df_partioned = spark.read.parquet("fhvhv/2019/01")
    return (df_partioned,)


@app.cell
def _(df_partioned):
    df_partioned.printSchema()
    return


@app.cell
def _(df_partioned):
    df_partioned.select("pickup_datetime", "dropoff_datetime") \
                .filter(df_partioned.hvfhs_license_num == "HV0003").show()
    return


@app.cell
def _(spark):
    for y in [2020,2021]:
        for i in range(1,13):
            df_green = spark.read \
                    .parquet(f"data/raw/green/{y}/{i:02d}")
            df_green.repartition(4) \
                .write.parquet(f"data/raw/green/{y}/{i:02d}/pq")
        
    return


@app.cell
def _(spark):
    for year in [2020,2021]:
        for m in range(1,13):
            df_yellow = spark.read \
                    .parquet(f"data/raw/yellow/{year}/{m:02d}")
            df_yellow.repartition(4) \
                .write.parquet(f"data/raw/yellow/{year}/{m:02d}/pq")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
