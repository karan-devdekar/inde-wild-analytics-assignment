import functions_framework
import pandas as pd
from google.cloud import storage, bigquery
import io
import re

# --- 1. Specific Transformation Logic ---
# These functions ensure data is in the right format BEFORE BQ sees it.

def clean_blinkit(df):
    """Refined logic for Blinkit-Sales data."""
    # Standardize Date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Validation: Remove rows with invalid dates or non-positive sales
    df = df.dropna(subset=['date'])
    df = df[(df['qty_sold'] > 0) & (df['mrp'] > 0)]
    
    # Standardization: Force IDs to string to prevent scientific notation in BQ
    id_cols = ['item_id', 'manufacturer_id', 'city_id']
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
            
    # Cleaning: Standardize text columns (Strip spaces and use Title Case)
    text_cols = ['item_name', 'manufacturer_name', 'city_name', 'category']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
            
    return df

def clean_zepto(df):
    """Refined logic for Zepto data (Format: DD-MM-YYYY)."""
    # Parse Dates (Zepto uses Day-Month-Year)
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['Date'])
    
    # Logic: Validate Sales and MRP
    # Note the specific Zepto column names
    df = df[(df['Sales (Qty) - Units'] > 0) & (df['MRP'] > 0)]
    
    # Standardize IDs as strings (Crucial for EAN and UUIDs)
    id_cols = ['EAN', 'SKU Number', 'Manufacturer ID']
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
            
    # Standardize Text (Title Case for categories and cities)
    text_cols = ['SKU Name', 'SKU Category', 'SKU Sub Category', 'Brand Name', 'City']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
            
    return df

def clean_nykaa(df):
    """Refined logic for Nykaa Online data."""
    # Standardize Date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    # Logic: Only keep rows with positive quantity and price
    df = df[(df['Total Qty'] > 0) & (df['Selling Price'] > 0)]

    # Standardize IDs and Codes as strings
    id_cols = ['SKU Code', 'seller_code']
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Standardize Text (Handle multiple category levels)
    text_cols = ['Display Name', 'Company Name', 'brand', 'SKU Name', 
                 'Category L1', 'Category L2', 'Category L3', 'Platform']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    return df

def clean_myntra(df):
    """Refined logic for Myntra data."""
    # Convert integer date (20260123) to proper Datetime
    df['order_created_date'] = pd.to_datetime(df['order_created_date'].astype(str), format='%Y%m%d', errors='coerce')
    df = df.dropna(subset=['order_created_date'])

    # Logic: Validate Sales and Revenue
    df = df[(df['sales'] > 0) & (df['mrp_revenue'] > 0)]

    # Standardize IDs as strings
    df['style_id'] = df['style_id'].astype(str)

    # Standardize Text
    text_cols = ['style_name', 'business_unit', 'brand_type', 'po_type', 'article_type', 'brand', 'gender']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    return df

# --- 2. BigQuery Column Name Standardizer ---
def standardize_bq_columns(df):
    """
    Renames columns to be BQ compliant:
    Example: 'Sales (Qty) - Units' -> 'sales_qty_units'
    """
    def clean_name(name):
        # 1. Replace all non-alphanumeric chars with underscore
        name = re.sub(r'[^a-zA-Z0-9]', '_', name)
        # 2. Replace multiple underscores with a single one
        name = re.sub(r'_+', '_', name)
        # 3. Trim underscores from ends and lowercase
        return name.strip('_').lower()

    df.columns = [clean_name(col) for col in df.columns]
    return df

def run_final_transformation():
    """Executes the SQL Merge/Create for the final analytics table."""
    client = bigquery.Client()
    
    # Your SQL Script
    sql_query = """
    CREATE OR REPLACE TABLE `inde-wild-analytics.inde_wild_final.daily_sales_fact` AS 
    with accumulated_data as (
      --blinkit
      select 
      date(date) as date,
      concat(item_id,'-',item_name) as product_identifier,
      'Blinkit' as data_source,
      qty_sold AS total_units,
      mrp as total_revenue
      from `inde-wild-analytics.inde_wild_stg.stg_blinkit_sales`

      union all 

      --myntra
      select 
      date(order_created_date) as date,
      concat(style_id,'-',style_name) as product_identifier,
      'Myntra' as data_source,
      sales AS total_units,
      (mrp_revenue-vendor_disc) as total_revenue
      from `inde-wild-analytics.inde_wild_stg.stg_myntra_sales`

      union all 

      --nykaa
      select 
      date(date) as date,
      concat(sku_code,'-',sku_name) as product_identifier,
      'Nykaa' as data_source,
      total_qty AS total_units,
      selling_price as total_revenue
      from `inde-wild-analytics.inde_wild_stg.stg_nykaa_sales`

      union all 

      --zepto
      select 
      date(date) as date,
      concat(sku_number,'-',sku_name) as product_identifier,
      'Zepto' as data_source,
      sales_qty_units AS total_units,
      mrp as total_revenue
      from `inde-wild-analytics.inde_wild_stg.stg_zepto_sales`
    )
    select 
    date,
    product_identifier,
    data_source,
    sum(total_units) as total_units,
    sum(total_revenue) as total_revenue
    from accumulated_data
    group by 1,2,3
    """
    
    print("Starting final transformation for daily_sales_fact...")
    query_job = client.query(sql_query)
    query_job.result()  # Wait for the table to be created
    print("final_daily_sales_fact table updated successfully.")

# --- 3. Routing & Merge Configuration ---
# Format: "keyword": (table_name, clean_function, [unique_merge_keys])
PLATFORM_MAP = {
    "blinkit": ("stg_blinkit_sales", clean_blinkit, ["date", "item_id", "city_id"]),
    "zepto": ("stg_zepto_sales", clean_zepto, ["date", "sku_number", "city"]),
    "nykaa": ("stg_nykaa_sales", clean_nykaa, ["date", "sku_code"]),
    "myntra": ("stg_myntra_sales", clean_myntra, ["order_created_date", "style_id"])
}

@functions_framework.cloud_event
def gcs_to_bigquery_trigger(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    if not file_name.endswith('.csv'): return

    target_table = None
    clean_func = None
    merge_keys = None
    for key, (table, func, keys) in PLATFORM_MAP.items():
        if key in file_name.lower():
            target_table = table
            clean_func = func
            merge_keys = keys
            break
            
    if not target_table: return

    # 1. Download and Process
    storage_client = storage.Client()
    content = storage_client.bucket(bucket_name).blob(file_name).download_as_bytes()
    df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig')
    
    df = df.drop_duplicates()
    df = clean_func(df)
    df = standardize_bq_columns(df)

    # 2. Setup BigQuery
    bq_client = bigquery.Client()
    dataset_id = f"{bq_client.project}.inde_wild_stg"
    final_table_id = f"{dataset_id}.{target_table}"
    temp_table_id = f"{final_table_id}_temp"

    # 3. Load to Temp Table (Always overwrite the temp table)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    bq_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config).result()

    # 4. Generate MERGE SQL
    cols = [f"`{c}`" for c in df.columns]
    join_condition = " AND ".join([f"T.{k} = S.{k}" for k in merge_keys])
    update_clause = ", ".join([f"T.{c} = S.{c}" for c in cols if c.strip('`') not in merge_keys])
    insert_cols = ", ".join(cols)
    insert_values = ", ".join([f"S.{c}" for c in cols])

    merge_query = f"""
    MERGE `{final_table_id}` T
    USING `{temp_table_id}` S
    ON {join_condition}
    WHEN MATCHED THEN
      UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols}) VALUES ({insert_values})
    """

    # 5. Execute Merge & Cleanup
    try:
        print(f"Merging data into {final_table_id}...")
        bq_client.query(merge_query).result()
    except Exception as e:
        # Fallback: If final table doesn't exist, create it by copying temp
        if "Not found: Table" in str(e):
            print("Final table missing. Creating it now...")
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            bq_client.load_table_from_dataframe(df, final_table_id, job_config=job_config).result()
        else:
            raise e

    bq_client.delete_table(temp_table_id, not_found_ok=True)
    print(f"Successfully processed and merged {file_name}.")

    try:
        run_final_transformation()
    except Exception as e:
        print(f"Error updating final table: {e}")