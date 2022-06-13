import sqlparse
from typing import Iterable, Optional, Sequence, Union
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class RedshiftSQLOperator(BaseOperator):
    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    # Databricks brand color (blue) under white text
    def __init__(
            self,
            sql: Union[str, Iterable[str]],
            redshift_conn_id: str = 'redshift_default',
            # parameters: Optional[dict] = None,
            autocommit: bool = True,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        # self.parameters = parameters
        self.autocommit = autocommit

    def get_hook(self) -> RedshiftSQLHook:
        return RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

    def execute(self, context: 'Context') -> None:
        self.log.info(f"Executing statement: {self.sql}")
        sql_stmts = sqlparse.split(self.sql)
        hook = self.get_hook()

        with hook.get_conn() as con:
            con.autocommit = self.autocommit
            with con.cursor() as cursor:
                for stmt in sql_stmts:
                    cursor.execute(stmt)
                    if self.autocommit is False: con.commit()