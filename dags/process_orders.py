import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

from utils import normalize_csv, load_csv_to_postgres

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'


create_stg_orders_sql = """
CREATE TABLE IF NOT EXISTS stg_orders (
    id VARCHAR NOT NULL,
    product_id VARCHAR,
    amount DECIMAL,
    total_price DECIMAL,
    status VARCHAR,
    event_time timestamp,
    processed_time timestamp
);

truncate stg_orders;
"""

create_dim_orders_sql = """
CREATE TABLE IF NOT EXISTS dim_orders (
    order_id VARCHAR NOT NULL,
    status VARCHAR,
    event_time timestamp,
    processed_time timestamp,
    start_time timestamp,
    end_time timestamp,
    UNIQUE(order_id, start_time)
);
"""

create_orders_sql = """
CREATE TABLE IF NOT EXISTS fact_orders_created (
    order_id VARCHAR NOT NULL,
    product_id VARCHAR,
    created_date_id VARCHAR,
    created_time timestamp,
    amount DECIMAL,
    total_price DECIMAL,
    processed_time timestamp,
    UNIQUE(order_id)
);
"""

transform_dim_orders_sql = """
WITH stg_orders_with_row_number AS (
     SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY id ORDER BY event_time) AS rn
      FROM stg_orders
), earliest_orders AS (
    SELECT * FROM stg_orders_with_row_number WHERE rn = 1
)
UPDATE dim_orders
SET end_time = '{{ ts }}'
FROM earliest_orders
WHERE earliest_orders.id = dim_orders.order_id
AND '{{ ts }}' >= dim_orders.start_time AND '{{ ts }}' < dim_orders.end_time
AND (earliest_orders.status <> dim_orders.status);

WITH ordered_stg_orders as (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id,status ORDER BY event_time) rn,
    LAST_VALUE(event_time) OVER(PARTITION BY id,status ORDER BY event_time) last_event_time
    FROM stg_orders
    order by id, event_time
), distinct_stg_orders as (
    select id, status, event_time, 
    event_time as start_time,
    last_event_time as end_time,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY event_time) rn
    from ordered_stg_orders where ordered_stg_orders.rn = 1
) ,new_records as (select 
    current_orders.id,
    current_orders.status,
    current_orders.event_time,
    current_orders.event_time as start_time,
    coalesce(next_orders.event_time, '%s') as end_time
from distinct_stg_orders current_orders left join distinct_stg_orders next_orders
on current_orders.id = next_orders.id and current_orders.rn = next_orders.rn -  1
) 
INSERT INTO dim_orders(order_id, status, event_time, processed_time, start_time, end_time)
SELECT id AS order_id,
    status,
    event_time,
    '{{ ts }}',
    start_time,
    end_time
FROM new_records
""" % default_end_time

transform_fact_orders_created_sql = """
INSERT INTO fact_orders_created(order_id, product_id, created_time, created_date_id, amount, total_price, processed_time)
SELECT stg_orders.id AS order_id,
    product_id,
    event_time as created_time,
    dim_dates.id as created_date_id,
    amount,
    total_price,
    '{{ ts }}'
FROM stg_orders
INNER JOIN dim_dates on dim_dates.datum = date(event_time)
ON CONFLICT(order_id) DO NOTHING
"""


with DAG(
    dag_id="process_orders",
    start_date=datetime.datetime(2020, 1, 1),  # datetime.datetime.now,
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:

    check_stg_orders_csv_readiness = BashSensor(
        task_id="check_stg_orders_csv_readiness",
        bash_command="""
            ls /data/raw/orders_{{ ds }}.csv
        """,
    )

    normalize_orders_csv = PythonOperator(
        task_id='normalize_orders_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/orders_{{ ds }}.csv",
            'target': "/data/stg/orders_{{ ds }}.csv"
        }
    )

    create_stg_orders_table = PostgresOperator(
        task_id="create_stg_orders_table",
        postgres_conn_id=connection_id,
        sql=create_stg_orders_sql,
    )

    load_orders_to_stg_orders_table = PythonOperator(
        task_id='load_orders_to_stg_orders_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/orders_{{ ds }}.csv",
            'table_name': 'stg_orders',
            'connection_id': connection_id
        },
    )

    create_dim_orders_table = PostgresOperator(
        task_id="create_dim_orders_table",
        postgres_conn_id=connection_id,
        sql=create_dim_orders_sql,
    )

    create_fact_orders_created_table = PostgresOperator(
        task_id="create_fact_orders_created_table",
        postgres_conn_id=connection_id,
        sql=create_orders_sql,
    )

    check_stg_orders_csv_readiness >> normalize_orders_csv >> create_stg_orders_table >> load_orders_to_stg_orders_table >> [
        create_dim_orders_table, create_fact_orders_created_table]

    transform_dim_orders_table = PostgresOperator(
        task_id="transform_dim_orders_table",
        postgres_conn_id=connection_id,
        sql=transform_dim_orders_sql,
    )

    create_dim_orders_table >> transform_dim_orders_table

    transform_fact_orders_created_table = PostgresOperator(
        task_id="transform_fact_orders_created_table",
        postgres_conn_id=connection_id,
        sql=transform_fact_orders_created_sql,
    )

    create_fact_orders_created_table >> transform_fact_orders_created_table
