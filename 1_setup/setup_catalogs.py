# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg; 
# MAGIC USE CATALOG fmcg;

# COMMAND ----------

# MAGIC %md
# MAGIC here the Silver and bronze are exclusively for child company SportsBar but, gold layer will contain data from both parent and child company. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fmcg.gold.fact_orders;