# Databricks notebook source
# Input data and working folders locations
MY_VOLUME="/Volumes/dbx_course/source/files"
SOURCE_LOCATION=f"{MY_VOLUME}/source/"
TARGET_FOLDER=f"{MY_VOLUME}/target/"
# Define paths for data access

# Core data paths
sales_path = f"{SOURCE_LOCATION}/ecommerce/sales/sales.delta"
users_path = f"{SOURCE_LOCATION}/source/ecommerce/users/users.delta"
events_path = f"{SOURCE_LOCATION}/source/ecommerce/events/events.delta"
products_path = f"{SOURCE_LOCATION}/source/products/products.delta"

# Working directories - for lab exercises
working_dir = f"{MY_VOLUME}/target"


# COMMAND ----------

from types import SimpleNamespace
DA = SimpleNamespace(
    paths = SimpleNamespace(
        datasets = MY_VOLUME,
        working_dir=working_dir,
        sales=sales_path,
        events=events_path,
        users=users_path,
        products=products_path,
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dbx_course; -- this is like a Database in Snowflake
# MAGIC USE CATALOG dbx_course;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS source;
# MAGIC CREATE SCHEMA IF NOT EXISTS target;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS dbx_course.source.files
# MAGIC

# COMMAND ----------

# Check if data already exists before copying
try:
    files = dbutils.fs.ls(SOURCE_LOCATION)
    if len(files) > 0:
        print("Source data already exists, skipping copy")
    else:
        raise Exception("Data files already present in {SOURCE_LOCATION} - No copying needed")
except:
    print("Copying data from S3...")
    #spark.conf.set("fs.s3a.bucket.dbx-data-public.aws.credentials.provider", 
    #               "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    dbutils.fs.cp("s3a://dbx-data-public/v03/", SOURCE_LOCATION, recurse=True)
    print(f"Done! Source files are in {SOURCE_LOCATION}")

# COMMAND ----------

try:
    spark.sql("DROP VIEW IF EXISTS sales;")
except:
    spark.sql("DROP TABLE IF EXISTS sales;")

try:
    spark.sql("DROP VIEW IF EXISTS users;")
except:
    spark.sql("DROP TABLE IF EXISTS users;")

try:
    spark.sql("DROP VIEW IF EXISTS products;")
except:
    spark.sql("DROP TABLE IF EXISTS products;")

try:
    spark.sql("DROP VIEW IF EXISTS events;")
except:
    spark.sql("DROP TABLE IF EXISTS events;")

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

# Set up spark configuration for SQL access to data paths
setup_spark_conf()

# COMMAND ----------

# Reset working directory for lab exercises
reset_working_dir()

# COMMAND ----------

displayHTML("✅ Classroom setup complete! 🎉")
displayHTML(f"<br/>")
displayHTML(f"✅ Catalog 'dbx_course' present")
displayHTML(f"✅ Working directory present: {TARGET_FOLDER} - Path stored in variable <pre>working_dir</pre> 📁")
displayHTML(f"<br/>")
displayHTML(f"Data paths:")
displayHTML(f"- Users data: {users_path}")
displayHTML(f"- Events data: {events_path}")
displayHTML(f"- Products data: {products_path}")
displayHTML(f"- Sales data: {sales_path}")
