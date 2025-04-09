from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ReadParquetFromHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Define the Parquet file path in HDFS
parquet_file_path = "hdfs://localhost:9000/user/hadoop/raw_employee_data/part-00000-8cd6bb89-ba3a-4840-9a4f-487ddb34ff51-c000.snappy.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(parquet_file_path)

# Show the data
df.show(truncate=False)

# Print Schema
df.printSchema()
