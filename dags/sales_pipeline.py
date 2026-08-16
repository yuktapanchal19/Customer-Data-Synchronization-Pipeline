from airflow.exceptions import AirflowException
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake .hooks.snowflake  import SnowflakeHook
import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)

def create_table():

    create_query = """
    CREATE TABLE IF NOT EXISTS RAW_SALES (
        order_id INT,
        product VARCHAR,
        quantity INT,
        unit_price FLOAT,
        sale_date DATE,
        region VARCHAR,
        loaded_at TIMESTAMP
    );
    """

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    hook.run(create_query)

    logger.info("Table RAW_SALES ready.")

def clear_todays_data():
     
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    delete_query = """
    DELETE FROM RAW_SALES
    WHERE loaded_at::DATE = CURRENT_DATE();
    """

    cursor.execute(delete_query)

    deleted_rows = cursor.rowcount

    logger.info(f"{deleted_rows} rows deleted from RAW_SALES.")

def load_csv_to_snowflake():

    csv_path = "data/sales_data.csv"

    if not os.path.exists(csv_path):
        raise AirflowException(
            f"CSV file not found : {csv_path}"
        )
    
    df = pd.read_csv(csv_path)

    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_default"
    )

    conn = hook.get_conn()
    cursor = conn.cursor()
    rows_loaded = 0

    insert_query = """
    INSERT INTO RAW_SALES (
        order_id,
        product,
        quantity,
        unit_price,
        sale_date,
        region,
        loaded_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    current_time = datetime.now()

    for _,row in df.iterrows():
        cursor.execute(
            insert_query,
            (
                int(row['order_id']),
                row['product'],
                int(row['quantity']),
                float(row['unit_price']),
                row['sale_date'],
                row['region'],
                current_time
            )
        )
        
        rows_loaded += 1

    conn.commit()

    logger.info(
        f"Loaded {rows_loaded} rows into RAW_SALES."
    )

def transform_in_snowflake():

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS SALES_SUMMARY (
        region VARCHAR,
        total_orders INT,
        total_revenue FLOAT,
        avg_order_value FLOAT,
        summary_date DATE
    );
    """
    cursor.execute(create_table_query)

    logger.info("SALES_SUMMARY table ready.")

    delete_query = """
    DELETE FROM SALES_SUMMARY
    WHERE summary_date = CURRENT_DATE();
    """

    cursor.execute(delete_query)

    deleted_rows = cursor.rowcount

    logger.info(
        f"Deleted {deleted_rows} existing summary rows for today."
    )

    insert_summary_query = """
    INSERT INTO SALES_SUMMARY (
        region,
        total_orders,
        total_revenue,
        avg_order_value,
        summary_date
    )
    SELECT
        region,
        COUNT(*) AS total_orders,
        SUM(quantity * unit_price) AS total_revenue,
        AVG(quantity * unit_price) AS avg_order_value,
        CURRENT_DATE()
    FROM RAW_SALES
    WHERE loaded_at::DATE = CURRENT_DATE()
    GROUP BY region;
    """
    cursor.execute(insert_summary_query)

    inserted_rows = cursor.rowcount

    conn.commit()

    logger.info(
        f"Written {inserted_rows} region summary rows into SALES_SUMMARY."
    )

def quality_check():
    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_default"
    )

    row_count_query = """
    select count(*) from RAW_SALES
    where loaded_at :: DATE = CURRENT_DATE();
    """

    row_count = hook.get_first(row_count_query)[0]

    if row_count == 0:
        logger.error(
            "FAIL: No rows loaded into RAW_SALES today."
        )
        raise AirflowException(
            "Quality Check Failed: RAW_SALES contains 0 rows for today."
        )
    else:
        logger.info(
            f"PASS: RAW_SALES contains {row_count} rows for today."
        )
    
    null_check_query = """
    select count(*)
    from RAW_SALES
    where product is null
    or unit_price is null;
    """

    null_count = hook.get_first(null_check_query)[0]

    if null_count > 0:
        logger.warning(
            f"WARNING: Found {null_count} rows with NULL values."
        )
    else:
        logger.info(
            "PASS: No NULL values found in product or unit_price."
        )

    negative_price_query = """
    select count(*)
    from RAW_SALES
    where unit_price < 0;
    """

    negative_count = hook.get_first(negative_price_query)[0]

    if negative_count > 0:
        logger.error(
            f"FAIL: Found {negative_count} rows with negative prices."
        )
        raise AirflowException(
            "Quality Check Failed: Negative unit_price values detected."
        )
    else:
        logger.info(
            "PASS: No negative unit_price values found."
        )
def print_report():

    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_default"
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    summary_query = """
    select 
        region,
        total_orders,
        total_revenue,
        avg_order_value
    from SALES_SUMMARY
    order by region;
    """
    cursor.execute(summary_query)
    results = cursor.fetchall()

    total_query = """
    SELECT
        SUM(total_revenue)
    FROM SALES_SUMMARY
    """
    cursor.execute(total_query)
    grand_total = cursor.fetchone()[0]

    logger.info("========== SALES REPORT ==========")

    for row in results:

        region = row[0]
        total_orders = row[1]
        total_revenue = row[2]
        avg_order_value = row[3]

        logger.info(
            f"""
        Region           : {region}
        Total Orders     : {total_orders}
        Total Revenue    : ${total_revenue:.2f}
        Avg Order Value  : ${avg_order_value:.2f}
        ---------------------------------------------
        """
        )

    logger.info(
        f"GRAND TOTAL REVENUE: ${grand_total:.2f}"
    )

    logger.info("========== END OF REPORT ==========")
 
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "daily_sales_loader",
    schedule = "@daily",
    catchup = False, 
    default_args=default_args,
    tags=["sales", "snowflake"]
) as dag:
    
    create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table
    )
    
    clear_todays_data_task = PythonOperator(
    task_id="clear_todays_data",
    python_callable=clear_todays_data
    )
    
    load_csv_to_snowflake_task = PythonOperator(
    task_id="load_csv_to_snowflake",
    python_callable=load_csv_to_snowflake
    )
    
    transform_in_snowflake_task = PythonOperator(
    task_id="transform_in_snowflake",
    python_callable=transform_in_snowflake
    )
    quality_check_task = PythonOperator(
        task_id = "quality_check",
        python_callable = quality_check
    )
    print_report_task = PythonOperator(
    task_id = "print_report",
    python_callable = print_report
    )

    create_table_task >> clear_todays_data_task >> load_csv_to_snowflake_task >> transform_in_snowflake_task >> quality_check_task >> print_report_task