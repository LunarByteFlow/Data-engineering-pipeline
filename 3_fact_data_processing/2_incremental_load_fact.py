# Databricks notebook source
# MAGIC %md
# MAGIC Import Required Libraries

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/mahnoortauseef2022@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-db-484395054876-us-east-1-an/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)


# define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

df = spark.read.options(header= True, inferSchema = True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp",F.current_timestamp()).select("*","_metadata.file_name","_metadata.file_size")
print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

df.write.format("delta").option("delta.enableChangeDataFeed","true").mode("append").saveAsTable(bronze_table)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just the arrived incremental data

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving files from source to processed directory

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

df_orders = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.staging_{data_source};")
df_orders.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# 1. keep only rows where order_qty is present
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# 2. Clean customer_id -> keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
    .otherwise("999999")
    .cast("string")
)

# 3. Remove weekday name from the date text. 
# "Tuesday", July 01, 2025 -> July 01, 2025
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,/s*","")
)

# 4. Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date","yyyy/MM/dd"),
        F.try_to_date("order_placement_date","dd-MM-yyyy"),
        F.try_to_date("order_placement_date","dd/MM/yyyy"),
        F.try_to_date("order_placement_date","MMMM dd, yyyy")
    )
)

# 5. Drop Duplicates
df_orders = df_orders.dropDuplicates(["order_id","order_placement_date","customer_id","product_id","order_qty"])

# 5. convert product_id to string
df_orders = df_orders.withColumn('product_id',F.col('product_id').cast('string'))
 

# COMMAND ----------

# Check what is the minimum and maximum date
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join with Products

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = df_orders.join(df_products,on="product_id",how = "inner").select(df_orders["*"],df_products["product_code"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join with the main parent table. 

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed","true"
    ).option("mergeSchema","true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"),"silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just arrived incremental data. 

# COMMAND ----------

# stagging for incremental data

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

df_gold.show(2)

# COMMAND ----------

df_gold.count()

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

df_gold.show(2)

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {gold_table}")

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("gold").merge(df_gold.alias("source"), "gold.date = source.date AND gold.order_id = source.order_id AND gold.product_code = source.product_code AND gold.customer_code = source.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## merging with Parent Company
# MAGIC Note: We want data for monthly level but child data is on daily level. 

# COMMAND ----------

# df_child = your incremental daily loads
df_child = spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")

incremental_month_df = df_child.select(
    F.trunc("date","MM").alias("start_month")
).distinct()
incremental_month_df.show()
incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------

monthly_table = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {catalog}.{gold_schema}.sb_fact_orders sbf
    INNER JOIN incremental_months m
        ON trunc(sbf.date, 'MM') = m.start_month
""")

print("Total Rows: ", monthly_table.count())
monthly_table.show(10)

# COMMAND ----------

monthly_table.select('date').distinct().orderBy('date').show()

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")   # month_start → date = first of month
)

df_monthly_recalc.show(10, truncate=False)

# COMMAND ----------

df_monthly_recalc.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly_recalc.alias("child_gold"), "parent_gold.date = date_format(child_gold.date, 'MM/dd/yyyy') AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.silver.staging_orders;