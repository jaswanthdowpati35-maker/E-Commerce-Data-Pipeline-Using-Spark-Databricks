### Silver to Gold: Building BI Ready Tables
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row
catalog_name = "ecommerce"
### Products 
df_products = spark.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.table(f"{catalog_name}.silver.slv_category")
df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")
display(spark.sql("select * from v_products limit 5"))
display(spark.sql('select * from v_category limit 5'))
display(spark.sql('select * from v_brands limit 5'))

# Make sure we’re on the right catalog
spark.sql(f"USE CATALOG {catalog_name}")
%sql

-- Build brands×category mapping and write Gold table
CREATE OR REPLACE TABLE gold.gld_dim_products AS

WITH brands_categories AS (
  SELECT
    b.brand_name,
    b.brand_code,
    c.category_name,
    c.category_code
  FROM v_brands b
  INNER JOIN v_category c
  ON
    b.category_code = c.category_code
)
SELECT
  p.product_id,
  p.sku,
  p.category_code,
  COALESCE(bc.category_name, 'Not Available') AS category_name,
  p.brand_code,
  COALESCE(bc.brand_name, 'Not Available')   AS brand_name,
  p.color,
  p.size,
  p.material,
  p.weight_grams,
  p.length_cm,
  p.width_cm,
  p.height_cm,
  p.rating_count,
  p.file_name,
  p.ingest_timestamp
FROM v_products p
LEFT JOIN brands_categories bc
  ON p.brand_code = bc.brand_code;
### Customers
# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  

country_state_map
# 1 Flatten country_state_map into a list of Rows
rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]        
# 2️ Create mapping DataFrame
df_region_mapping = spark.createDataFrame(rows)

# Optional: show mapping
df_region_mapping.show(truncate=False)
df_silver = spark.table(f'{catalog_name}.silver.slv_customers')
display(df_silver.limit(5))
df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')

df_gold = df_gold.fillna({'region': 'Other'})

display(df_gold.limit(5))
# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")
### Date/Calendar
df_silver = spark.table(f'{catalog_name}.silver.slv_calendar')
display(df_silver.limit(5))
df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

# Add month name (e.g., 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
)

display(df_gold.limit(5))

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))
# write table to gold layer
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")
%sql

DESCRIBE EXTENDED ecommerce.gold.gld_dim_date;
