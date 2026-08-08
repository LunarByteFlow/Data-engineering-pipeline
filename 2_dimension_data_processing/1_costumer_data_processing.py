# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/mahnoortauseef2022@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
base_path = f's3://sportsbar-db-484395054876-us-east-1-an/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = spark.read.format('csv').load(base_path)
df = (
    spark.read.format("csv")
    .option("header",True)
    .option("inferSchema",True)
    .load(base_path)
    .withColumn("read_timestamp", f.current_timestamp())
    .select("*","_metadata.file_name","_metadata.file_size")
)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Now we will directly write all the data being fetched from s3 buckets to the bronze layer in out databricks architecture. 

df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Layer

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC so bronze contains raw data coming from applications, in our case coming from the se object store, which we clean and it will be stored in the silver layer

# COMMAND ----------

# find duplicate customers
df_duplicates = df_bronze.groupBy("customer_id").count().filter(f.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped:', df_silver.count())

# COMMAND ----------

# Now trim the leading whitespaces
display(df_silver.filter(f.col('customer_name') != f.trim(f.col("customer_name"))))


# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    f.trim(f.col("customer_name"))
)

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

# typos --> correct names
city_mapping = {
    'Bengaluruu':'Bengaluru',
    'Bengalore':'Bengaluru',
    'Hyderabadd':'Hyderabad',
    'Hyderbad':'Hyderabad',
    'NewDelhi': 'New Delhi',
    'NewDheli':'New Delhi',
    'NewDelhee':'New Delhi'

}
allowed = ['Bengaluru','Hyderabad','New Delhi']
df_silver = (
    df_silver
    .replace(city_mapping,subset = ['city'])
    .withColumn(
        'city',
        f.when(f.col('city').isNull(),None)
        .when(f.col("city").isin(allowed),f.col("city"))
        .otherwise(None)
    )
)
df_silver.select('city').distinct().show()

# COMMAND ----------

# Fixing the cases of alphabets
df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# customer name case fix
df_silver = df_silver.withColumn(
    'customer_name',
    f.when(f.col('customer_name').isNull(),None)
    .otherwise(f.initcap('customer_name'))
)

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# see the rows where city is NULL
df_silver.filter(f.col('city').isNull()).show(truncate = False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(f.col('customer_name').isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# Business confirmation Note: City corrections confirmed by buriness team
customer_city_fix = {
    # Sprintx Nutrition
    789403:'New Delhi',
    # Zenathlete Foods
    789420:'Bengaluru',
    # Primefuel Nutrition
    789521:'Hyderabad',
    # Recovery Lane
    789603: 'Hyderabad'
}

df_fix = spark.createDataFrame(
    [(k,v) for k,v in customer_city_fix.items()],
    ['customer_id','fixed_city']
)
display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix,'customer_id','left')
    .withColumn(
        'city',
        # replace null with fixed city
        f.coalesce('city','fixed_city') 
    )
    .drop('fixed_city')
)
display(df_silver)

# COMMAND ----------



# COMMAND ----------

df_silver = df_silver.withColumn('customer_id',f.col('customer_id').cast('string'))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: 'CustomerName-City' or 'CustomerName-Unknown'
    .withColumn(
        'customer',
        f.concat_ws('-','customer_name',f.coalesce(f.col('city'),f.lit('Unknown')))
    )
    # Static attributes aligned with parent data model
    .withColumn('market',f.lit('India'))
    .withColumn('platform',f.lit('Sports Bar'))
    .withColumn('channel',f.lit('Acquisition'))
)
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
.format('delta')\
.option('delta.enableChangeDataFeed','true')\
.option('mergeSchema','true')\
.mode('overwrite')\
.saveAsTable(f'{catalog}.{silver_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ** Gold layer processing now

# COMMAND ----------

df_silver = spark.sql(f'SELECT * FROM {catalog}.{silver_schema}.{data_source};')
# For gold layer take the required columns only. 
# customer_id, customer_name, city, read_timestamp, file_name, customer, market, platform, channel.
df_gold = df_silver.select('customer_id','customer_name','city','customer','market','platform','channel')


# COMMAND ----------

# now we write this table to the gold layer. 
df_gold.write\
.format('delta')\
.option('delta.enableChangeDataFeed','true')\
.mode('overwrite')\
.saveAsTable(f'{catalog}.{gold_schema}.sb_dim_{data_source}')
# display the gold table
display(df_gold.limit(10))

# COMMAND ----------

# now we have to build one final table by combining the parent and child company gold layer data.
delta_table = DeltaTable.forName(spark,'fmcg.gold.dim_customers')
df_child_customers = spark.table('fmcg.gold.sb_dim_customers').select(
    f.col('customer_id').alias('customer_code'),
    'customer',
    'market',
    'platform',
    'channel'
)

# COMMAND ----------

delta_table.alias('target').merge(
    source = df_child_customers.alias('source'),
    condition = 'target.customer_code = source.customer_code'
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()