from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import closing

class MySqlToPostgresOperator(BaseOperator):
    """Selects data from a MySQL database and inserts that data into a
    PostgreSQL database. Cursors are used to minimize memory usage for large
    queries.
    """

    template_fields = ("sql", "postgres_table", "params")
    template_ext = (".sql",)
    ui_color = "#944dff"  # cool purple

    @apply_defaults
    def __init__(
        self,
        params: None,
        sql: str,
        mysql_conn_id: str = "mysql_default",
        postgres_table: str = "",
        postgres_conn_id: str = "postgres_default",
        rows_chunk: int = 5000,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if params is None:
            params = {}
        self.sql = sql
        self.mysql_conn_id = mysql_conn_id
        self.postgres_table = postgres_table
        self.postgres_conn_id = postgres_conn_id
        self.params = params
        self.rows_chunk = rows_chunk

    def execute(self, context):
        """Establish connections to both MySQL & PostgreSQL databases, open
        cursor and begin processing query, loading chunks of rows into
        PostgreSQL. Repeat loading chunks until all rows processed for query.
        """
        source = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        target = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with closing(source.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self.sql, self.params)
                target_fields = [x[0] for x in cursor.description]
                row_count = 0
                rows = cursor.fetchmany(self.rows_chunk)
                while len(rows) > 0:
                    row_count += len(rows)
                    target.insert_rows(
                        self.postgres_table,
                        rows,
                        target_fields=target_fields,
                        commit_every=self.rows_chunk,
                    )
                    rows = cursor.fetchmany(self.rows_chunk)
                self.log.info(
                    f"{row_count} row(s) inserted into {self.postgres_table}."
                )