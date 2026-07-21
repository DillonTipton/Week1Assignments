from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, BooleanType
import pyspark.sql.functions as func

# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("TitanicToIceberg")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://rev-spark-609375805055-us-east-2-an/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)

# Create schemas
orders_schema = StructType([
    StructField('order_id', StringType()),
    StructField('customer_id', StringType()),
    StructField('product_id', StringType()),
    StructField('order_date', DateType()),
    StructField('ship_date', DateType()),
    StructField('quantity', IntegerType()),
    StructField('unit_price', FloatType()),
    StructField('discount_pct', FloatType()),
    StructField('total_amount', FloatType()),
    StructField('payment_method', StringType()),
    StructField('order_status', StringType())
])

products_schema = StructType([
    StructField('product_id', StringType()),
    StructField('product_name', StringType()),
    StructField('category', StringType()),
    StructField('brand', StringType()),
    StructField('price', FloatType()),
    StructField('cost', FloatType()),
    StructField('stock_quantity', IntegerType()),
    StructField('weight_kg', FloatType()),
    StructField('created_date', DateType()),
    StructField('is_active', BooleanType())
])

customers_schema = StructType([
    StructField('customer_id', StringType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('phone', StringType()),
    StructField('signup_date', DateType()),
    StructField('country', StringType()),
    StructField('state', StringType()),
    StructField('postal_code', StringType()),
    StructField('is_active', BooleanType()),
    StructField('loyalty_points', IntegerType())
])

# Read data
orders_df = spark.read.csv(
    "s3://rev-spark-609375805055-us-east-2-an/orders.csv",
    schema=orders_schema,
    header=True
)

products_df = spark.read.csv(
    "s3://rev-spark-609375805055-us-east-2-an/products.csv",
    schema=products_schema,
    header=True
)

customers_df = spark.read.csv(
    "s3://rev-spark-609375805055-us-east-2-an/customers.csv",
    schema=customers_schema,
    header=True
)

# Display the DataFrame's schema (column names and data types)
# to verify the data was loaded correctly.
orders_df.printSchema()
products_df.printSchema()
customers_df.printSchema()

# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.
# This isn't as simple as using the connector between spark and snowflake, but it allows the code to be interoperable with many different softwares
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")

# We need to clean the data before loading, this is an ETL pipeline

# Clean customer's table
# customer ID needs to be integer value

# Throwing out bad rows is the easiest method
customers_df_clean = customers_df.drop_duplicates()
# need to strip whitespace from all columns

# Caveman way
#customers_df_clean = customers_df_clean.withColumn(func.col("first_name"), func.trim(func.col("first_name")))
#customers_df_clean = customers_df_clean.withColumn(func.col("last_name"), func.trim(func.col("last_name")))
#customers_df_clean = customers_df_clean.withColumn(func.col("email"), func.trim(func.col("email")))
#customers_df_clean = customers_df_clean.withColumn(func.col("phone_number"), func.trim(func.col("phone_number")))

# More efficient way
customers_df_clean = customers_df_clean.select([
    func.trim(func.col(c)).alias(c) if t == "string" else func.col(c) 
    for c, t in customers_df_clean.dtypes
])

# Regex pattern for a standard email
email_pattern = r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)"
# Keep only strings that match the email pattern, otherwise return null
customers_df_clean = customers_df_clean.withColumn(
    "email",
    func.when(
        func.col("email").rlike(email_pattern),
        func.col("email")
    )
)

phone_pattern = r"^(\+?\d{1,3}[\s.-]?)?(\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}$"
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    func.when(
        func.col("phone").rlike(phone_pattern),
        func.col("phone")
    )
)

# Strip unnecessary puctuation from country
customers_df_clean = customers_df_clean.withColumn(
    "country",
    func.regexp_replace("country", r"[^a-zA-Z0-9\s]", "")
)

# Capitalize state names
customers_df_clean = customers_df_clean.withColumn(
    "state",
    func.upper(func.col("state"))
)

# Ensure loyalty points can't be negative
customers_df_clean = customers_df_clean.withColumn(
    "loyalty_points",
    func.when(func.col("loyalty_points") < 0, 0).otherwise(func.col("loyalty_points"))
)

# More we could clean up, but for now this will suffice

customers_df_clean = customers_df_clean.dropna()

# Clean Orders
orders_df_clean = orders_df.drop_duplicates()

# Clean IDs
order_id_pattern = r"^\d{6}$"
orders_df_clean = orders_df_clean.withColumn(
    "order_id",
    func.when(
        func.col("order_id").rlike(order_id_pattern),
        func.col("order_id")
    )
)

customer_id_pattern = r"^\d{4}$"
orders_df_clean = orders_df_clean.withColumn(
    "customer_id",
    func.when(
        func.col("customer_id").rlike(customer_id_pattern),
        func.col("customer_id")
    )
)

product_id_pattern = r"^\w\d{4}$"
orders_df_clean = orders_df_clean.withColumn(
    "product_id",
    func.when(
        func.col("product_id").rlike(product_id_pattern),
        func.col("product_id")
    )
)

# Clean price, quantity, and total amount, take absolute value of negative values
orders_df_clean = orders_df_clean.withColumn(
    "quantity",
    func.when(func.col("quantity") < 0, func.abs(func.col("quantity"))).otherwise(func.col("quantity"))
)

orders_df_clean = orders_df_clean.withColumn(
    "unit_price",
    func.when(func.col("unit_price") < 0.0, func.abs(func.col("unit_price"))).otherwise(func.col("unit_price"))
)

orders_df_clean = orders_df_clean.withColumn(
    "total_amount",
    func.when(func.col("total_amount") < 0.0, func.abs(func.col("total_amount"))).otherwise(func.col("total_amount"))
)

orders_df_clean = orders_df_clean.dropna()

# Clean Products
products_df_clean = products_df.drop_duplicates()

# Clean ID
# Pattern is earlier
products_df_clean = products_df_clean.withColumn(
    "product_id",
    func.when(
        func.col("product_id").rlike(product_id_pattern),
        func.col("product_id")
    )
)

# Clean cost and price
products_df_clean = products_df_clean.withColumn(
    "cost",
    func.when(func.col("cost") < 0, func.abs(func.col("cost"))).otherwise(func.col("cost"))
)

products_df_clean = products_df_clean.withColumn(
    "price",
    func.when(func.col("price") < 0.0, func.abs(func.col("price"))).otherwise(func.col("price"))
)

# Clean stock, just set to 0 if below
products_df_clean = products_df_clean.withColumn(
    "stock_quantity",
    func.when(func.col("stock_quantity") < 0.0, 0).otherwise(func.col("stock_quantity"))
)

products_df_clean = products_df_clean.dropna()

#TODO: Copy this file into S3 once finished with it
#TODO: Clone the latest Iceberg cluster for demo. Be sure tha AWS Glue is enabled.
#TODO: Then SSH into node from powershell using the given link
#TODO: Create Snowflake tables from them, use Snowflake_to_S3_with_external_catalog for reference, then create 5 different queries from them

# Write the DataFrame as an Iceberg table.
(
    orders_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.orders"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    products_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.products"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

(
    customers_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.customers"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

# Query the newly created Iceberg table to verify that the
# data was written successfully.
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.customers
LIMIT 10
""").show()

spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.orders
LIMIT 10
""").show()

spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.products
LIMIT 10
""").show()

# Stop the Spark session and release cluster resources.
spark.stop()