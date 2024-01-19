from collections.abc import Iterable
from dataclasses import KW_ONLY, InitVar, dataclass, field
from typing import List, Union

import mariadb

from taiwan_stock_analysis.database.datatype import DataType


@dataclass
class MariadbTable:
    """
    A wrapper used to represent Mariadb table.

    This class create SQL query string like CREATE TABLE, INSERT etc.

    Attributes:
        _columns (List[DataType]): A list composed of DataType used to
        represent a Mariadb table.
        _insert_query (str): The SQL query string to insert data into
        table.
        _table_query str: The SQL query string to create a table if not
        exists.
        _table_name (str): The name of MariaDB table.

    Members:
    - table_name (str): The name of MariaDB table.
    - table_query (str): The SQL query string to create a table if not
      exists.
    - insert_query (str): The SQL query string to insert data into
      table.

    Raises:
    - ValueError: This exception is raised if any members is assigned
      to a new value.
    """

    _columns: List[DataType] = field(init=False)
    _insert_query: str = field(init=False)
    _table_query: str = field(init=False)
    _table_name: str
    columns: InitVar[Union[DataType, Iterable[DataType]]]

    def __post_init__(self, columns: Union[DataType, Iterable[DataType]]):
        """
        Initialize the MariadbTable class after its __init__ method.

        Parameters:
        - columns (Union[DataType, Iterable[DataType]]): Either a
            single DataType instance or an iterable of DataType
            instances representing the columns of the MariaDB table.

        Raises:
        - ValueError:
            - If 'columns' is not provided or is an empty iterable.
            - If any element in 'columns' is not of type DataType.
            - If there are duplicate column names.

        Sets up the _insert_query and _table_query attributes for
        database operations.

        The _insert_query is a SQL query string for inserting
        records into the table, with support for ON DUPLICATE KEY
        UPDATE for primary key conflicts.

        The _table_query is a SQL query string for creating the table
        with its columns and an optional PRIMARY KEY constraint.
        """
        if not columns:
            raise ValueError(
                "You must provide at least one data type for MariaDB table."
            )
        elif isinstance(columns, Iterable):
            self._columns = list(columns)
        else:
            self._columns = [columns]
        allow_class = DataType
        if not all(
            isinstance(column, allow_class) for column in self._columns
        ):
            raise ValueError(
                "All elements in 'columns' must be "
                f"type {allow_class.__name__}."
            )
        if len(self._columns) != len(set(self._columns)):
            raise ValueError("Duplicate column names are not allowed.")
        primary = [
            column.name for column in self._columns if column.primary_key
        ]
        placeholders = ", ".join(["?" for _ in range(len(self))])
        query = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
        columns = [column.definition() for column in self._columns]
        if primary:
            columns.append(f"PRIMARY KEY ({', '.join(primary)})")
            query = (
                query
                + " ON DUPLICATE KEY UPDATE "
                + ", ".join(
                    f"{column} = VALUES({column})" for column in primary
                )
            )
        self._insert_query = query
        query = ", ".join(columns)
        self._table_query = (
            f"CREATE TABLE IF NOT EXISTS {self._table_name} ({query})"
        )

    def __len__(self) -> int:
        return len(self._columns)

    @property
    def table_name(self):
        """Return name of SQL table."""
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        raise RuntimeError("Can't assign a new value to table_name.")

    @property
    def table_query(self):
        """Return SQL query to create table."""
        return self._table_query

    @table_query.setter
    def table_query(self, value):
        raise RuntimeError("Can't assign a new value to table_query.")

    @property
    def insert_query(self):
        """Return SQL query to insert data into table."""
        return self._insert_query

    @insert_query.setter
    def insert_query(self):
        raise RuntimeError("Can't assign a new value to insert_query.")


@dataclass
class MariadbClient:
    """
    A client wrapper for MariaDB.

    This wrapper aims for SQL operation like: create table, insert data.

    Attributes:
        table(MariadbTable): The table we want to operate.
    """

    _table: MariadbTable
    _: KW_ONLY
    user: InitVar[str]
    password: InitVar[str]
    host: InitVar[str]
    database: InitVar[str]

    def __post_init__(
        self,
        user: str,
        password: str,
        host: str,
        database: str,
    ):
        """
        Initialize the MariadbClient class after its __init__ method.

        Parameters:
        - user (str): The user name to connect to MariaDB.
        - password (str): The password to connect to MariaDB.
        - host (str): The host name to connect to MariaDB.
        - database (str): The database name to connect to MariaDB.

        Sets up the _connection and _cursor for database operations.
        If table doesn't existed, create it.
        """
        self._connection = mariadb.connect(
            user=user,
            password=password,
            host=host,
            database=database,
        )
        self._cursor = self._connection.cursor()
        self.execute(self._table.table_query)

    def __len__(self) -> int:
        return len(self._table)

    def cursor(self):
        """Return cursor of database connection."""
        return self._connection.cursor()

    def execute(self, sql: str, parameters=(), /):
        """
        Execute a single SQL statement.

        Parameters:
        - sql (str): A single SQL statement.
        - parameters: Python values to bind to placeholders in sql.
        """
        self._cursor.execute(sql, parameters)

    def insert(self, *data):
        """
        Insert data into table.

        Parameters:
        - data (tuple): Data to insert to table.

        Raises:
        - ValueError: This exception is raised if the length of data is
          not equal to columns of Mariadb table.
        """
        if len(data) != len(self):
            raise ValueError(
                f"Data items do not match table '{self._table_name}' columns."
            )
        query = self._table.insert_query
        self._cursor.execute(query, data)

    def commit(self):
        """Commit to SQL database."""
        self._connection.commit()

    def close(self):
        """Close connection and cursor of SQL database."""
        self._cursor.close()
        self.commit()
        self._connection.close()
