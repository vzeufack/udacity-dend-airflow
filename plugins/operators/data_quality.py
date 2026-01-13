from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks if checks is not None else []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Supported comparison operators
        ops = {
            "==": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le
        }

        for check in self.checks:
            sql = check["sql"]
            expected = check["expected_result"]
            comparison = check.get("comparison", "==")
            description = check.get("description", sql)

            self.log.info(f"Running data quality check: {description}")
            self.log.info(f"Executing SQL: {sql}")

            records = redshift.get_records(sql)

            if not records or not records[0]:
                raise ValueError(f"Data quality check failed: No results returned for query: {sql}")

            actual = records[0][0]

            # Perform comparison
            if comparison not in ops:
                raise ValueError(f"Unsupported comparison operator: {comparison}")

            if not ops[comparison](actual, expected):
                raise ValueError(
                    f"Data quality check FAILED: {description}\n"
                    f"SQL returned {actual}, expected {comparison} {expected}"
                )

            self.log.info(
                f"Data quality check PASSED: {description} "
                f"(actual: {actual}, expected: {comparison} {expected})"
            )

        self.log.info("All data quality checks passed successfully.")