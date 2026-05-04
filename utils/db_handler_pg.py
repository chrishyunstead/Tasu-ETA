import boto3
import pandas as pd
import psycopg2
import psycopg2.extras


class PostgresDBHandler:
    def __init__(self):
        ssm = boto3.client("ssm")

        param_names = {
            "pg_user": "/Sample/Postgres/USER",
            "pg_password": "/Sample/Postgres/PASSWORD",
            "pg_host": "/Sample/Postgres/HOST",
            "pg_database": "/Sample/Postgres/DATABASE",
        }

        self.pg_user = self._get_ssm_parameter(ssm, param_names["pg_user"])
        self.pg_password = self._get_ssm_parameter(ssm, param_names["pg_password"])
        self.pg_host = self._get_ssm_parameter(ssm, param_names["pg_host"])
        self.pg_database = self._get_ssm_parameter(ssm, param_names["pg_database"])
        self.pg_port = 5432

    def _get_ssm_parameter(self, ssm, name):
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]

    def fetch_data(self, query):
        """PostgreSQL에서 데이터 조회 후 DataFrame 반환"""
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                dbname=self.pg_database,
                user=self.pg_user,
                password=self.pg_password,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
            return pd.DataFrame(results)

        except Exception as e:
            print(f"[PostgresDBHandler] Error fetching data: {e}")
            return None

        finally:
            if conn:
                conn.close()