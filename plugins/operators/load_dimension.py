from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 target_table="",
                 sql_statement="",
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_statement = sql_statement
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info(f"Truncating Redshift table {self.target_table}")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")

        formatted_sql = f"""
            INSERT INTO {self.target_table}
            {self.sql_statement}
        """

        self.log.info(f"Loading data into Redshift table {self.target_table}")
        redshift.run(formatted_sql)

        self.log.info(f"Load into {self.target_table} completed successfully.")
