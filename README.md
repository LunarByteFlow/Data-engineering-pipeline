# FMCG Data Lakehouse Project

## 📌 What is this project?
This project takes raw FMCG (Fast-Moving Consumer Goods) business data—such as customer lists, product details, prices, and daily sales orders—and cleans it up so it can be used for business reporting.

It processes data using **Databricks** and **PySpark**, following the **Medallion Architecture** (Bronze ➔ Silver ➔ Gold) to transform messy raw data into structured tables.



## 🔄 How the Data Pipeline Works

### 1. Bronze Layer (Raw Ingestion)
* Ingests raw `.csv` files landing in an **AWS S3** bucket.
* Stores the raw data as-is without changing it.
* Adds tracking information like file name and load timestamps.

### 2. Silver Layer (Cleaning & Fixing Errors)
* **Standardizes Dates:** Fixes mixed date formats (e.g., `2025/12/01`, `01-12-2025`, `July 01, 2025`) and strips out weekday names.
* **Fixes Typos & Case:** Fixes city name typos (e.g., `Bengaluruu` ➔ `Bengaluru`), product category names, and extra spaces].
* **Handles Bad Data:** Corrects negative prices, drops duplicate records, and assigns default values (like `999999`) for missing IDs.

### 3. Gold Layer (Merging with Parent Company)
* **Daily-to-Monthly Rollup:** Aggregates daily order numbers into monthly totals to match the parent company's reporting structure.
* **Data Merging (Upserts):** Uses Delta Lake `MERGE` statements to insert new records or update existing ones without creating duplicate rows.

---

## 📁 File Structure

* **`1_setup/utilities.py`**: Sets up catalog names, schemas, and shared variables.
* **`1_customer_data_processing.py`**: Cleans customer names, cities, and generates customer dimension tables.
* **`2_products_data_processing.py`**: Fixes product categories, extracts product variants, and builds the product dimension table.
* **`3_pricing_data_processing.py`**: Validates prices and picks the correct yearly price per product.
* **`1_full_load_fact.py`**: Loads the initial historical batch of order sales.
* **`2_incremental_load_fact.py`**: Processes daily new incoming orders and merges them into the main dataset.
* **`sql/copy_into_gold_fact.sql`**: SQL query for bulk loading CSV files directly into Delta tables using `COPY INTO`.

---

## 🛠️ Tech Stack
* **Platform:** Databricks
* **Storage:** AWS S3, Delta Lake
* **Languages:** PySpark, SQL
