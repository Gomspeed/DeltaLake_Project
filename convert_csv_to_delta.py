from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Build Spark session with Delta
builder = SparkSession.builder \
    .appName("CSVtoDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Path to your CSV file
csv_path = r"C:\Users\Gomspeed\Downloads\archive\oil and gas.csv"

# Read CSV
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Save as Delta Lake table
delta_path = r"C:\Users\Gomspeed\Downloads\data\oil_delta"
df.write.format("delta").mode("overwrite").save(delta_path)

print("✅ CSV converted to Delta at:")
print(delta_path)
