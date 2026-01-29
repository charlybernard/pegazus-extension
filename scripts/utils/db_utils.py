"""
db_utils.py

A utility class to manage PostgreSQL operations including connection,
schema and table management, query execution, and data selection.

Features:
- Connect from a .ini config file
- Create/drop schema or table
- Execute SELECT and UPDATE/DDL queries
- Install PostGIS extension
- Structured logging

Dependencies:
- psycopg2
- configparser
"""

import psycopg2
import configparser
import logging
from psycopg2 import sql
import os

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresManager:
    def __init__(self, config_file: str):
        """
        Initializes the manager by connecting to PostgreSQL using a config file.

        Args:
            config_file (str): Path to a .ini file with [postgresql] section.
        """
        self.conn = self._connect_from_config(config_file)

    def _connect_from_config(self, config_file: str):
        if os.path.exists(config_file) and os.path.isfile(config_file):
            config = configparser.ConfigParser()
            config.read(config_file)
            print("Config file loaded.")
        else:
            raise FileNotFoundError(f"Config file not found: {config_file}")

        db_params = config['postgresql']
        conn_params = {
            'host': db_params.get('host', 'localhost'),
            'port': int(db_params.get('port', 5432)),
            'dbname': db_params.get('database', ''),
            'user': db_params.get('user', '')
        }

        password = db_params.get('password', None)
        if password:
            conn_params['password'] = password

        return psycopg2.connect(**conn_params)

    def execute_query(self, query: str, error_message: str = None, success_message: str = None):
        """
        Executes an SQL query with commit and error handling.

        Args:
            query (str): SQL query to execute.
            error_message (str, optional): Custom message on failure.
            success_message (str, optional): Custom message on success.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
            self.conn.commit()
            if success_message:
                logger.info(success_message)
        except Exception as e:
            self.conn.rollback()
            msg = error_message or f"❌ Error executing query: {e}"
            logger.error(msg)

    def fetch_one(self, query: str):
        """
        Executes a SELECT query and returns a single result row.

        Args:
            query (str): SQL SELECT query.

        Returns:
            tuple: One row of results, or None if no results or error.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchone()
        except Exception as e:
            logger.error(f"❌ Error executing fetch_one: {e}")
            return None

    def fetch_all(self, query: str):
        """
        Executes a SELECT query and returns all result rows.

        Args:
            query (str): SQL SELECT query.

        Returns:
            list of tuple: All result rows, or empty list if no results or error.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
        except Exception as e:
            logger.error(f"❌ Error executing fetch_all: {e}")
            return []

    def create_schema(self, schema_name: str):
        query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
        self.execute_query(
            query.as_string(self.conn),
            error_message=f"❌ Failed to create schema '{schema_name}'",
            success_message=f"✅ Schema '{schema_name}' created or already exists."
        )

    def drop_schema(self, schema_name: str, cascade: bool = False):
        query = sql.SQL("DROP SCHEMA IF EXISTS {} {}").format(
            sql.Identifier(schema_name),
            sql.SQL("CASCADE") if cascade else sql.SQL("")
        )
        self.execute_query(
            query.as_string(self.conn),
            success_message=f"✅ Schema '{schema_name}' dropped."
        )

    def create_postgis_extension(self):
        self.execute_query(
            "CREATE EXTENSION IF NOT EXISTS postgis;",
            success_message="✅ PostGIS extension ensured."
        )

    def create_table(self, schema: str, table: str, columns: list):
        """
        Creates a table with given column definitions.

        Args:
            schema (str): Schema name.
            table (str): Table name.
            columns (list of str): Column definitions (e.g., ["id SERIAL", "name TEXT"]).
        """
        col_sql = ", ".join(columns)
        query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(col_sql)
        )
        self.execute_query(
            query.as_string(self.conn),
            success_message=f"✅ Table '{schema}.{table}' created or already exists."
        )

    def drop_table(self, schema: str, table: str):
        query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        self.execute_query(
            query.as_string(self.conn),
            success_message=f"✅ Table '{schema}.{table}' dropped."
        )

    def close(self):
        """Closes the PostgreSQL connection."""
        if self.conn:
            self.conn.close()
            logger.info("🔌 Connection closed.")
