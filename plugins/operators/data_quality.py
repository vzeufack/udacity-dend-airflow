from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables if tables is not None else []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f"Running data quality check on table: {table}")

            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed: No results returned for table {table}")

            row_count = records[0][0]

            if row_count < 1:
                raise ValueError(f"Data quality check failed: Table {table} contains 0 rows")

            self.log.info(f"Table {table} passed row check with {row_count} rows.")
        
        self.log.info("All data quality checks passed successfully.")
