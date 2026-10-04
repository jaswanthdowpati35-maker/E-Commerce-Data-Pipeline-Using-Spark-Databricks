## Bronze to Silver: Data Cleaning and Transformation for Dimension Tables
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = 'ecommerce'
### Brands
df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)
df_silver = df_bronze.withColumn("brand_name", F.trim(F.col("brand_name")))
df_silver.show(10)
df_silver = df_silver.withColumn("brand_code", F.regexp_replace(F.col("brand_code"), r'[^A-Za-z0-9]', ''))
df_silver.show(10)
df_silver.select("category_code").distinct().show()
# Anomalies dictionary
anomalies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

# PySpark replace is easy
df_silver = df_silver.replace(to_replace=anomalies, subset=["category_code"])

# ✅ Show results
df_silver.select("category_code").distinct().show()
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_brands)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_brands")
### Category
df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")

df_bronze.show(10)
df_duplicates = df_bronze.groupBy("category_code").count().filter(F.col("count") > 1)
display(df_duplicates)
df_silver = df_bronze.dropDuplicates(['category_code'])
display(df_silver)
df_silver = df_silver.withColumn("category_code", F.upper(F.col("category_code")))
display(df_silver)
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")
### Products
# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")
display(df_bronze.limit(5))
Check `weight_grams` (contains 'g')
# Check weight_grams column
df_bronze.select("weight_grams").show(5, truncate=False)
# replace 'g' with ''
df_silver = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver.select("weight_grams").show(5, truncate=False)
Check `length_cm` (comma instead of dot)
df_silver.select("length_cm").show(3)
# replace , with .
df_silver = df_silver.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver.select("length_cm").show(3)
`category_code` and `brand_code` are in lower case. we need to make it all upper case
df_silver.select("category_code", "brand_code").show(2)
# convert category_code and brand_code to upper case
df_silver = df_silver.withColumn(
    "category_code",
    F.upper(F.col("category_code"))
).withColumn(
    "brand_code",
    F.upper(F.col("brand_code"))
)
df_silver.select("category_code", "brand_code").show(2)
Spelling mistakes in `material` column
df_silver.select("material").distinct().show()
# Fix spelling mistakes
df_silver = df_silver.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)
df_silver.select("material").distinct().show()    
Negative values in `rating_count`
df_silver.filter(F.col('rating_count')<0).select("rating_count").show(3)

# Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(F.lit(0))  # if null, replace with 0
)
# Check final cleaned data

df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_dim_products)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")
### Customers
# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)
Handle NULL values in `customer_id` column
null_count = df_bronze.filter(F.col("customer_id").isNull()).count()
null_count
# There are 300 null values in customer_id column. Display some of those
df_bronze.filter(F.col("customer_id").isNull()).show(3)
# Drop rows where 'customer_id' is null
df_silver = df_bronze.dropna(subset=["customer_id"])

# Get row count
row_count = df_silver.count()
print(f"Row count after droping null values: {row_count}")
Handle NULL values in `phone` column
null_count = df_silver.filter(F.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 
df_silver.filter(F.col("phone").isNull()).show(3)
### Fill null values with 'Not Available'
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# sanity check (If any nulls still exist)
df_silver.filter(F.col("phone").isNull()).show()
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")
### Calendar/Date
# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(3)
Remove Duplicates
# Find duplicate rows in the DataFrame
duplicates = df_bronze.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)
# Remove duplicate rows
df_silver = df_bronze.dropDuplicates(['date'])

# Get row count
row_count = df_silver.count()

print("Rows After removing Duplicates: ", row_count)
`day_name` normalize casing
# Capitalize first letter of each word in day_name
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

df_silver.show(5)
Convert negative `week_of_year` to positive
df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))  # Convert negative to positive

df_silver.show(3)
Enhance `quarter` and `week_of_year` column
df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

df_silver.show(3)
Rename columns
# Rename a column
df_silver = df_silver.withColumnRenamed("week_of_year", "week")
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")
