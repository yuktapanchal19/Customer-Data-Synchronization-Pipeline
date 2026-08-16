from email.policy import default
from sqlalchemy import exists
from airflow.decorators import dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)
file_path = "data/customers_data.csv"

def alert_on_failure(context):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    print(f"ALERT: Task {task_id} in DAG {dag_id} has failed. Check the logs.")

def validate_file():

    if not os.path.exists(file_path):
        raise AirflowException(
            "customers_data.csv not found"
        )
    logger.info("CSV file found — proceeding with load.")
    return file_path

def create_and_load():

    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_default"
    )
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_query = """
    CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    customer_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    signup_date DATE,
    plan VARCHAR,
    loaded_at TIMESTAMP);
    """
    cursor.execute(create_query)
    logger.info("Table RAW_CUSTOMERS ready.")

    delete_query = """
    DELETE FROM RAW_CUSTOMERS
    WHERE loaded_at::DATE = CURRENT_DATE();
    """
    cursor.execute(delete_query)
    logger.info("rows deleted from RAW_CUSTOMERS.")  

    df = pd.read_csv(file_path)
    rows_inserted = 0

    insert_query = """
    INSERT INTO RAW_CUSTOMERS (
        customer_id,
        name,
        email,
        city,
        signup_date,
        plan,
        loaded_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
    """
    for _, row in df.iterrows():
        cursor.execute(
            insert_query,
            (
                int(row['customer_id']),
                row['name'],
                row['email'],
                row['city'],
                row['signup_date'],
                row['plan']
            )
        )
        rows_inserted += 1 
    conn.commit()
    logger.info(f"Total rows inserted: {rows_inserted}")

def enrich_customers():
    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_default"
    )
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_query = """
    CREATE TABLE IF NOT EXISTS ENRICHED_CUSTOMERS (
    customer_id INT,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    signup_date DATE,
    plan VARCHAR,
    customer_tier VARCHAR,
    days_since_signup INT,
    loaded_at TIMESTAMP)
    """
    cursor.execute(create_query)
    logger.info("Table ENRICHED_CUSTOMERS ready.")
    
    delete_query = """
    DELETE FROM ENRICHED_CUSTOMERS
    WHERE loaded_at::DATE = CURRENT_DATE()
    """
    cursor.execute(delete_query)

    insert_query = """
    INSERT INTO ENRICHED_CUSTOMERS
    SELECT 
        customer_id,
        name,
        email,
        city,
        signup_date,
        plan,
        CASE
            WHEN plan = 'premium'
            THEN 'Premium Tier'
            ELSE 'Basic Tier'
        END AS customer_tier,
        DATEDIFF('day', signup_date, CURRENT_DATE()) AS days_since_signup,
        CURRENT_TIMESTAMP() AS loaded_at

    FROM RAW_CUSTOMERS
    WHERE loaded_at::DATE = CURRENT_DATE()
    """
    cursor.execute(insert_query)
    logger.info("Enriched rows created successfully.")
    conn.commit()

def quality_check ():
    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_default"
    )

    null_check_query = """
    SELECT COUNT(*) FROM ENRICHED_CUSTOMERS 
    WHERE (name IS NULL OR email IS NULL) AND loaded_at::DATE = CURRENT_DATE()
    """

    duplicate_check_query = """
    SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM ENRICHED_CUSTOMERS 
    WHERE loaded_at::DATE = CURRENT_DATE()
    """
    null_count = hook.get_first(null_check_query)[0]
    duplicate_count = hook.get_first(duplicate_check_query)[0]

    logger.info(f"NULL check result: {null_count}")
    logger.info(f"Duplicate check result: {duplicate_count}")

    if null_count == 0 and duplicate_count == 0:
        return "all_clear"
    return "data_warning"

def all_clear():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )
    sql = """
    SELECT COUNT(*)
    FROM ENRICHED_CUSTOMERS
    WHERE loaded_at::DATE = CURRENT_DATE()
    """

    total = hook.get_first(sql)[0]

    logger.info("Quality checks passed — customer data is clean.")
    logger.info(f"Total enriched rows today: {total}")

def data_warning():
    logger.warning("WARNING: Data quality issues detected in customer load.")

def final_summary():
    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_default"
    )

    summary_sql = """
    SELECT
        COUNT(*),
        COUNT(CASE WHEN customer_tier='Premium Tier' THEN 1 END),
        COUNT(CASE WHEN customer_tier='Basic Tier' THEN 1 END),
        AVG(days_since_signup)
    FROM ENRICHED_CUSTOMERS
    WHERE loaded_at::DATE = CURRENT_DATE()
    """
    result = hook.get_first(summary_sql)

    city_sql = """
    SELECT city, COUNT(*)
    FROM ENRICHED_CUSTOMERS
    WHERE loaded_at::DATE = CURRENT_DATE()
    GROUP BY city
    ORDER BY COUNT(*) DESC
    LIMIT 1
    """

    city_result = hook.get_first(city_sql)

    logger.info(f"Total customers: {result[0]}")
    logger.info(f"Premium Tier customers: {result[1]}")
    logger.info(f"Basic Tier customers: {result[2]}")
    logger.info(f"Average days_since_signup: {result[3]}")
    logger.info(f"Top city: {city_result[0]}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "on_failure_callback" : alert_on_failure
}

@dag(
    dag_id = "customer_sync",
    schedule = "@daily",
    catchup = False,
    default_args = default_args
)

def customer_pipeline():

    validate = PythonOperator(
        task_id = "validate_file",
        python_callable = validate_file
    )

    load = PythonOperator(
        task_id="create_and_load",
        python_callable=create_and_load
    )

    enrich = PythonOperator(
        task_id="enrich_customers",
        python_callable=enrich_customers
    )

    quality = BranchPythonOperator(
        task_id="quality_check",
        python_callable=quality_check
    )

    clear = PythonOperator(
        task_id="all_clear",
        python_callable=all_clear
    )

    warning = PythonOperator(
        task_id="data_warning",
        python_callable=data_warning
    )

    summary = PythonOperator(
        task_id="final_summary",
        python_callable=final_summary,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    validate >> load >> enrich >> quality >> [clear, warning] >> summary


dag = customer_pipeline()

