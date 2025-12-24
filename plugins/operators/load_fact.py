from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 target_table="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_statement = sql_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = f"""
            INSERT INTO {self.target_table}
            {self.sql_statement}
        """

        self.log.info(f"Loading data into Redshift table {self.target_table}")
        redshift.run(formatted_sql)

        self.log.info(f"Load into {self.target_table} completed successfully.")
