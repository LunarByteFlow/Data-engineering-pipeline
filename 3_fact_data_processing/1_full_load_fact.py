# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/mahnoortauseef2022@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-db-484395054876-us-east-1-an/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print(f"Base Path",base_path)
print(f"Landing Path",landing_path)
print(f"Processed Path",processed_path)

# define bronze tables
bronze_table = f"{catalog}.{bronze_schema}.sb_fact_{data_source}"
silver_table = f"{catalog}.{silver_schema}.sb_fact_{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"


# COMMAND ----------

# DBTITLE 1,Cell 5
df = spark.read.options(header = True,inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows",df.count)
display(df)

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("append")\
    .saveAsTable(bronze_table)

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
# MAGIC Silver Layer processing

# COMMAND ----------

df_orders = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders.show(2)

# COMMAND ----------

# 1. Handling the order_qty NULL case
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# 2. Clean customer_id + keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"),F.col("customer_id"))
    .otherwise("999999")
    .cast("string")
)

# 3. remove weekdays from dates
# df_orders = df_orders.withColumn(
#     "order_placement_date",
#     F.regexp_replace(F.col("order_placement_date"),r"^[A-Za-z]+,\s*","")
# )

df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(
        F.col("order_placement_date"), 
        r"^\s*(?:Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat|Sun)[a-z]*\.?,?\s*", 
        "", 
        # Case-insensitive flag ensures "tuesday" or "TUESDAY" both match
    )
)

# 4. Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date","yyyy/MM/dd"),
        F.try_to_date("order_placement_date","dd-MM-yyyy"),
        F.try_to_date("order_placement_date","dd/MM/yyyy"),
        F.try_to_date("order_placement_date","MMMM dd, yyyy"),
    )
)

# 5. Drop Duplicates
df_orders = df_orders.dropDuplicates(["order_id","order_placement_date","customer_id","product_id","order_qty"])

# 6. Convert Product_id to string
df_orders = df_orders.withColumn('product_id',F.col("product_id").cast("string"))

# COMMAND ----------

# Check what is the maximum and minimum date. 
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

display(df_orders.limit(20))

# COMMAND ----------

# getting that long product code. 
df_products = spark.table("fmcg.silver.products")
display(df_products.limit(5))

# COMMAND ----------

# Now we want the product_code in our orders table as was playing a nice role as a unique identifier. so we will use it. we will do join on product code. 
df_joined = df_orders.join(df_products, on="product_id",how="inner").select(df_orders["*"],df_products['product_code'])
display(df_joined.limit(10))

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed","true"
    ).option("mergeSchema","true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark,silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"),"silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold 
# MAGIC Now we will start processing for the gold layer. 

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer, product_code as product, order_qty as quantity FROM {silver_table}")
display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 19
if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed","true"
    ).option("mergeSchema","true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark,gold_table)
    gold_delta.alias("gold").merge(df_joined.alias("source"),"gold.order_placement_date = source.order_placement_date AND gold.order_id = source.order_id AND gold.product_code = source.product_code AND gold.customer_id = source.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ### merge with parent Company

# COMMAND ----------

# MAGIC %md
# MAGIC Now we want to merge with the parent company.
# MAGIC now the parent company data is month level where as the child company data is day level. as now we are doing the merge of the fact table which is the orders table we have to keep this in mind. 

# COMMAND ----------

# DBTITLE 1,Cell 21
# so now load the child data first. 
df_child = spark.sql(f"SELECT order_placement_date, product_code, customer_id, order_qty FROM {gold_table}")
df_child.show()

# COMMAND ----------

df_child.count()

# COMMAND ----------

# First we will change the data to first day of the month. 
# 2025-07-1 --> 2025-07-1
# 2025-07-13 --> 2025-07-1

# to extract the month we will use this function
df_monthly = (
    df_child
    # 1. Get month start date (e.g., 2025-11-30 -> 2025-11-01)
    .withColumn("month_start",F.trunc("order_placement_date","MM"))

    # 2. Group at monthly grain by month_start column by prduct_code + costumer_id
    .groupBy("month_start","product_code","customer_id")
    .agg(
        F.sum("order_qty").alias("order_qty")
    )

    # 3. Rename the month_start back to date cause that is what the parent table calls it.
    .withColumnRenamed("month_start","date")

)
display(df_monthly)


# COMMAND ----------

df_monthly.count()

# COMMAND ----------

# Rename 'customer_id' to 'customer_code'
df_monthly = df_monthly.withColumnRenamed("customer_id", "customer_code")
df_monthly = df_monthly.withColumnRenamed("order_qty", "sold_quantity")


# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

# COMMAND ----------


gold_parent_delta.alias("parent_gold").merge(df_monthly.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()




# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

