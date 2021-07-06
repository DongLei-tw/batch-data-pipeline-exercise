import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'


def normalize_csv(ts, **kwargs):
    import csv
    source_filename = kwargs['source']
    target_filename = kwargs['target']
    header_skipped = False
    with open(source_filename, newline='') as source_file:
        with open(target_filename, "w", newline='') as target_file:
            reader = csv.reader(source_file, delimiter=',')
            writer = csv.writer(target_file, delimiter="\t",
                                quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if not header_skipped:
                    header_skipped = True
                    continue
                row.append(ts)
                writer.writerow(row)
    return target_filename


def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.bulk_load(table_name, csv_filepath)
    return table_name


with DAG(
    dag_id="process_products",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    check_csv_readiness = BashSensor(
        task_id="check_csv_readiness",
        bash_command="""
            ls /data/raw/products_{{ ds }}.csv
        """,
    )

    normalize_products_csv = PythonOperator(
        task_id="normalize_products_csv",
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/products_{{ ds }}.csv",
            'target': "/data/stg/products_{{ ds }}.csv"
        }
    )

    create_stging_products_table = PostgresOperator(
        task_id="create_staging_products_table",
        postgres_conn_id=connection_id,
        sql="""
            CREATE TABLE IF NOT EXISTS staging_products (
                id VARCHAR NOT NULL UNIQUE,
                title VARCHAR,
                category VARCHAR,
                price DECIMAL,
                processed_time timestamp
            );
            TRUNCATE staging_products;
          """
    )

    load_products_to_staging_table = PythonOperator(
        task_id='load_products_to_staging_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/products_{{ ds }}.csv",
            'table_name': "staging_products"
        },
    )

    create_products_dim_table = PostgresOperator(
        task_id="create_dim_products_table",
        postgres_conn_id=connection_id,
        sql="""
            CREATE TABLE IF NOT EXISTS dim_products (
                id VARCHAR NOT NULL,
                title VARCHAR,
                category VARCHAR,
                price DECIMAL,
                processed_time timestamp,
                start_time timestamp,
                end_time timestamp,
                UNIQUE(id, start_time)
            );
        """,
    )

    transform_dim_products_table = PostgresOperator(
        task_id="load_dim_products_table",
        postgres_conn_id=connection_id,
        sql="""
            UPDATE dim_products
                SET end_time = '{{ ts }}'
            FROM staging_products
            WHERE
                staging_products.id = dim_products.id
                AND '{{ ts }}' >= dim_products.start_time AND '{{ ts }}' < dim_products.end_time
                AND (dim_products.title <> staging_products.title OR dim_products.category <> staging_products.category OR dim_products.price <> staging_products.price);

            WITH sc as (
                SELECT * FROM dim_products
                WHERE '{{ ts }}' >= dim_products.start_time and '{{ ts }}' < dim_products.end_time
            )
            INSERT INTO dim_products(id, title, category, price, processed_time, start_time, end_time)
            SELECT staging_products.id as id,
                staging_products.title,
                staging_products.category,
                staging_products.price,
                '{{ ts }}' AS processed_time,
                '{{ ts }}' AS start_time,
                '%s' AS end_time
            FROM staging_products
            WHERE staging_products.id NOT IN (select id from sc);
            """ % default_end_time
    )

    check_csv_readiness >> normalize_products_csv >> create_stging_products_table >> load_products_to_staging_table >> create_products_dim_table >> transform_dim_products_table
