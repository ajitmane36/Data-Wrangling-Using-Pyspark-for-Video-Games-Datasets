# Video Game Sales And Steam Video Games - Dataset Exploration and Cleaning

# Installing pyspark and findspark library
!pip install pyspark
!pip install findspark

# Importing necessary libraries
import pyspark
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum
import sys
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Creating instance of spark
spark = SparkSession.builder.appName("Data Exploration and Cleaning").getOrCreate()
spark


# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Create a DynamicFrame from the "vgsales" dataset in S3
vgsales_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={"withHeader": True},
    connection_options={"paths": ["s3://vgindiggdatafinal/inputfolder/vgsales.csv"]}
)

# Create a DynamicFrame from the "vgtraffic" dataset in S3
vgtraffic_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={"withHeader": True},
    connection_options={"paths": ["s3://vgindiggdatafinal/inputfolder/steam-200k.csv"]}
)

# Video games sales dataset
vgsales_df.show()

# Video games traffic dataset
vgtraffic_df.show()

# Adding column names for video games traffic dataset
column_name=[ 'user-id', 'game-title', 'behavior-name', 'value','0']
vgtraffic_df = vgtraffic_df.toDF(*column_name)
vgtraffic_df.show()

# Data Inispection

# Checking number of rows and columns present in each dataset

# Check the number of rows and columns for vgsales_df
print(f"vgsales_df contains {vgsales_df.count()} rows and {len(vgsales_df.columns)} columns")

# Check the number of rows and columns for vgtraffic_df
print(f"vgtraffic_df contains {vgtraffic_df.count()} rows and {len(vgtraffic_df.columns)} columns")

# Checking schema of the datasets
vgsales_df.printSchema()
vgtraffic_df.printSchema()

# Understanding Variables

# Checking categorical and numerical variables from each dataset

# Categorical and numerical variables in the "Video Game sales" dataset
vgsales_categorical_cols = []
vgsales_numerical_cols = []

for column, dtype in vgsales_df.dtypes:
    if dtype == "string":
        vgsales_categorical_cols.append(column)
    elif dtype in ["int","double"]:
        vgsales_numerical_cols.append(column)

print(f"{len(vgsales_categorical_cols)} categorical columns present in 'Video Game sales' dataset: {vgsales_categorical_cols}")
print(f"{len(vgsales_numerical_cols)} Numerical columns present in 'Video Game sales' dataset: {vgsales_numerical_cols}")


# Categorical and numerical variables in the "Video Games traffic on steam" dataset
vgtraffic_categorical_cols = []
vgtraffic_numerical_cols = []

for column, dtype in vgtraffic_df.dtypes:
    if dtype == "string":
        vgtraffic_categorical_cols.append(column)
    elif dtype in ["byte", "short", "int", "long", "float", "double"]:
        vgtraffic_numerical_cols.append(column)

print(f"{len(vgtraffic_categorical_cols)} categorical columns present in 'Video Games traffic data' dataset: {vgtraffic_categorical_cols}")
print(f"{len(vgtraffic_numerical_cols)} numerical columns present in 'Video Games traffic data' dataset: {vgtraffic_numerical_cols}")

## Data Cleaning and Preprocessing

#[1] Handling Duplicated data

# Check for duplicates in the "Video Game sales" dataset
vgsales_duplicates = vgsales_df.dropDuplicates()
num_vgsales_duplicates = vgsales_duplicates.count()

if num_vgsales_duplicates > 0:
    print("Duplicates found in 'Video Game sales' dataset.")
    print("Number of duplicate rows:", num_vgsales_duplicates)
else:
    print("No duplicates found in 'Video Game sales' dataset.")

# Check for duplicates in the "Video Games traffic on steam" dataset
vgtraffic_duplicates = vgtraffic_df.dropDuplicates()
num_vgtraffic_duplicates = vgtraffic_duplicates.count()

if num_vgtraffic_duplicates > 0:
    print("Duplicates found in 'Video Games traffic on steam' dataset.")
    print("Number of duplicate rows:", num_vgtraffic_duplicates)
else:
    print("No duplicates found in 'Video Games traffic on steam' dataset.")


# Dropping duplicated

# Drop duplicates in the "Video Game sales" dataset
vgsales_duplicates_removed = vgsales_df.dropDuplicates()
num_vgsales_rows = vgsales_duplicates_removed.count()

print("Number of rows remaining after dropping duplicates in 'Video Game sales' dataset:", num_vgsales_rows)

# Drop duplicates in the "Video Games traffic on steam" dataset
vgtraffic_duplicates_removed = vgtraffic_df.dropDuplicates()
num_vgtraffic_rows = vgtraffic_duplicates_removed.count()

print("Number of rows remaining after dropping duplicates in 'Video Games traffic' dataset:", num_vgtraffic_rows)

# [2] Handling null/missing data

# Checking null values for each dataset

from pyspark.sql.functions import col, sum

# Calculate the number of nulls for each variable in the "Video Game sales" dataset
vgsales_null_counts = vgsales_duplicates_removed.select(*[sum(col(c).isNull().cast("int")).alias(c) for c in vgsales_duplicates_removed.columns])

# Calculate the number of nulls for each variable in the "Video Games traffic on steam" dataset
vgtraffic_null_counts = vgtraffic_duplicates_removed.select(*[sum(col(c).isNull().cast("int")).alias(c) for c in vgtraffic_duplicates_removed.columns])

# Create a table to display the variable name and number of nulls for the "Video Game sales" dataset
vgsales_null_table = spark.createDataFrame([(c, vgsales_null_counts.first()[c]) for c in vgsales_null_counts.columns], ["Variable", "Nulls"])

# Create a table to display the variable name and number of nulls for the "Video Games traffic on steam" dataset
vgtraffic_null_table = spark.createDataFrame([(c, vgtraffic_null_counts.first()[c]) for c in vgtraffic_null_counts.columns], ["Variable", "Nulls"])

# Show the null table for the "Video Game sales" dataset
print("Null values in 'Video Game sales' dataset:")
vgsales_null_table.show(truncate=False)

# Show the null table for the "Video Games traffic on steam" dataset
print("Null values in 'Video Games traffic on steam' dataset:")
vgtraffic_null_table.show(truncate=False)

# [3] Basic description of both dataset

# Display the schema information of the "Video Games traffic on steam" dataset
print("Schema information of 'Video Games traffic on steam' dataset:")
vgtraffic_duplicates_removed.printSchema()

# Display the schema information of the "Video Game sales" dataset
print("Schema information of 'Video Game sales' dataset:")
vgsales_duplicates_removed.printSchema()

# Remove the "0" column from the traffic dataset
vgtraffic_duplicates_removed = vgtraffic_duplicates_removed.drop("0")

# Convert the "Year" column in the sales dataset from string to integer
vgsales_duplicates_removed = vgsales_duplicates_removed.withColumn("Year", vgsales_duplicates_removed["Year"].cast("integer"))

# Print the schema of the modified "Video Games traffic" dataset
print("Schema of 'Video Games traffic on steam' dataset after removing the '0' column:")
vgtraffic_duplicates_removed.printSchema()

# Print the schema of the modified "Video Game sales" dataset
print("Schema of 'Video Game sales' dataset after converting the 'Year' column to integer:")
vgsales_duplicates_removed.printSchema()


# [4] Merge datasets

# DataFrames vgsales_duplicates_removed and vgtraffic_duplicates_removed

# Merge the datasets based on common columns
merged_df = vgsales_duplicates_removed.join(
    vgtraffic_duplicates_removed,
    vgsales_duplicates_removed["Name"] == vgtraffic_duplicates_removed["game-title"],
    "inner"
)

# Show the merged DataFrame
merged_df.show()

# [5] Checking correlation between Viewers and Global_Sales

# Optionally, perform additional analysis or computations on the merged data
correlation = merged_df.select("Global_Sales", "value").corr("Global_Sales", "value")

# Output the merged data and correlation result
merged_df.show(10)
print("Correlation between Global Sales and Viewers:", correlation)
