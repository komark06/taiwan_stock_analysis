import logging
import os
from collections.abc import Iterable
from dataclasses import KW_ONLY, InitVar, dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import List, Union

import mariadb


class NumericAttribute(Enum):
    """Represents attribute of numeric type."""

    UNSIGNED = auto()
    SIGNED = auto()
    ZEROFILL = auto()


UNSIGNED = NumericAttribute.UNSIGNED
SIGNED = NumericAttribute.SIGNED
ZEROFILL = NumericAttribute.ZEROFILL


@dataclass(eq=False)
class DataType:
    """
    Base class represents a data type in a database schema.

    Attributes:
        name (str): The name of the data type.
        primary_key (bool): Indicates if the data type is a primary key.
        Defaults to False.

    Methods:
    - definition(): Return SQL definition of data type.

    Raises:
    - NotImplementedError: This exception is raised if the
      'definition' method is not overridden.
    """

    name: str
    _: KW_ONLY
    primary_key: bool = False

    def definition(self) -> str:
        """
        Return SQL definition of data type.

        Raises:
        - NotImplementedError: This exception is raised if this method
          is not overridden.
        """
        raise NotImplementedError("subclass must implement this method.")


@dataclass(eq=False)
class Text(DataType):
    """Represents a TEXT data type in a database schema."""

    def definition(self) -> str:
        """
        Return SQL definition of data type.

        Returns:
            str: the SQL query of data type.
        """
        return f"{self.name} TEXT"


@dataclass(eq=False)
class VarChar(DataType):
    """
    Represents a VARCHAR data type in a database schema.

    Attributes:
        length (int): The length of the VARCHAR data type.
    """

    length: int

    def definition(self) -> str:
        """
        Return SQL definition of data type.

        Returns:
            str: the SQL query of data type.
        """
        return f"{self.name} VARCHAR({self.length}) CHARACTER SET utf8"


@dataclass(eq=False)
class NumericType(DataType):
    """
    Represents a numeric data type in a database schema.

    Attributes:
        attribute (NumericAttribute): The sign of the numeric type.
    """

    _: KW_ONLY
    attribute: NumericAttribute = SIGNED

    def determine_attr(self) -> str:
        """
        Return SQL query of the attribute of the numeric type.

        Raises:
        - ValueError: This exception is raised if 'attribute' is not
          member of NumericAttribute.

        Returns:
            str: String that represent attribute of the numeric type.
        """
        if self.attribute == ZEROFILL:
            return "ZEROFILL"
        elif self.attribute == UNSIGNED:
            return "UNSIGNED"
        elif self.attribute == SIGNED:
            return "SIGNED"
        name = self.__class__.__name__
        enum_name = ZEROFILL.__class__.__name__
        raise ValueError(
            f"The attribute of {name} must be a valid member of {enum_name}."
        )

    def definition(self) -> str:
        """
        Return the definition of the numeric type.

        Raises:
        - NotImplementedError: This exception is raised if
          'numeric_type' is not defined.

        Returns:
            str: The definition of numeric type.
        """
        if not hasattr(self, "numeric_type"):
            raise NotImplementedError("Subclass must define 'numeric_type'.")
        attribute = self.determine_attr()
        return f"{self.name} {self.numeric_type} {attribute}"


@dataclass(eq=False)
class TinyInt(NumericType):
    """
    Represents a TINYINT data type in a database schema.

    Attributes:
        numeric_type (str): String that represent numeric type.
    """

    numeric_type = "TINYINT"


@dataclass(eq=False)
class SmallInt(NumericType):
    """
    Represents a SMALLINT data type in a database schema.

    Attributes:
        numeric_type (str): String that represent numeric type.
    """

    numeric_type = "SMALLINT"


@dataclass(eq=False)
class Integer(NumericType):
    """
    Represents a INT data type in a database schema.

    Attributes:
        numeric_type (str): String that represent numeric type.
    """

    numeric_type = "INT"


@dataclass(eq=False)
class BigInt(NumericType):
    """
    Represents a BIGINT data type in a database schema.

    Attributes:
        numeric_type (str): String that represent numeric type.
    """

    numeric_type = "BIGINT"


@dataclass(eq=False)
class Decimal(NumericType):
    """
    Represents a DECIMAL data type in a database schema.

    Attributes:
        numeric_type (str): String that represent numeric type.
    """

    numeric_type = "DECIMAL"
    precision: int
    scale: int

    def definition(self) -> str:
        """
        Return the definition of the numeric type.

        Returns:
            str: The definition of numeric type.
        """
        return (
            f"{self.name} {self.numeric_type}({self.precision}, {self.scale})"
        )


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


class StockInfoPipeline:
    """
    A class for processing information of stock.

    Attributes:
        data_type (List[DateType]): A list composed of DataType used to
        represent a SQL table.
        table_name (str): The name of SQL table.
    """

    data_type = [
        VarChar("classification", 25),
        VarChar("symbol", 25, primary_key=True),
        VarChar("name", 25),
        VarChar("ISINCode", 25),
        VarChar("listing_date", 25),
        VarChar("market_category", 25),
        VarChar("industry_category", 25),
        VarChar("CFICode", 25),
        VarChar("remark", 25),
    ]
    table_name = "stock_info"

    def open_spider(self, spider):
        """
        Open the spider and initialize the MariaDB client.

        Parameters:
            spider: The spider instance.
        """
        table = MariadbTable(self.table_name, self.data_type)
        user = "root"
        password_file = os.getenv("MARIADB_ROOT_PASSWORD_FILE")
        if password_file:
            with open(password_file, "r") as file:
                password = file.read()
        else:
            password = os.getenv("MARIADB_ROOT_PASSWORD")
        host = os.getenv("MARIADB_HOST")
        database = os.getenv("MARIADB_DATABASE")
        self.client = MariadbClient(
            table,
            user=user,
            password=password,
            host=host,
            database=database,
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_item(self, item, spider):
        """
        Process the scraped item and insert it into the MariaDB table.

        Parameters:
            item: The item to be processed.
            spider: The spider instance.

        Returns:
            item: The processed item.
        """
        self.client.insert(*item.values())
        self.logger.info(item)
        return item

    def close_spider(self, spider):
        """
        Close MariaDB client connection.

        Parameters:
            spider: The spider instance.
        """
        self.client.close()


class DailyTradingPipeline:
    """
    A class for processing daily trading information of stock.

    Attributes:
        data_type (List[DateType]): A list composed of DataType used to
        represent a SQL table.
        table_name (str): The name of SQL table.
        check_data_type (List[DateType]): A list composed of DataType
        used to represent a SQL table for storing update time.
        table_name (str): The name of SQL table for storing update time.
    """

    data_type = {
        "symbol": ("SMALLINT", "UNSIGNED"),
        "year": ("SMALLINT", "UNSIGNED"),
        "month": ("TINYINT", "UNSIGNED"),
        "day": ("TINYINT", "UNSIGNED"),
        "volume": ("BIGINT", "UNSIGNED"),
        "value": ("BIGINT", "UNSIGNED"),
        "open": ("DECIMAL(8, 3)",),
        "highest": ("DECIMAL(8, 3)",),
        "lowest": ("DECIMAL(8, 3)",),
        "closing": ("DECIMAL(8, 3)",),
        "delta": ("VARCHAR(10)",),
        "transaction_volume": ("INTEGER",),
    }
    data_type = [
        SmallInt("symbol", attribute=UNSIGNED, primary_key=True),
        SmallInt("year", attribute=UNSIGNED, primary_key=True),
        TinyInt("month", attribute=UNSIGNED, primary_key=True),
        TinyInt("day", attribute=UNSIGNED, primary_key=True),
        BigInt("volume", attribute=UNSIGNED),
        BigInt("value", attribute=UNSIGNED),
        Decimal("open", 8, 3),
        Decimal("highest", 8, 3),
        Decimal("lowest", 8, 3),
        Decimal("closing", 8, 3),
        VarChar("delta", 10),
        Integer("transaction_volume"),
    ]
    table_name = "stock_daily_trading"
    check_data_type = [
        SmallInt("symbol", attribute=UNSIGNED, primary_key=True),
        SmallInt("year", attribute=UNSIGNED, primary_key=True),
        TinyInt("month", attribute=UNSIGNED, primary_key=True),
        Text("update_time"),
    ]
    check_table_name = "check_table"

    def _create_check_table(self):
        """Create check table if needed."""
        query = ", ".join(
            column.table_query() for column in self.check_data_type
        )
        self.cursor.execute(query)

    def open_spider(self, spider):
        """
        Open the spider and initialize the MariaDB client.

        It will create another table for storing update time.

        Parameters:
            spider: The spider instance.
        """
        table = MariadbTable(self.table_name, self.data_type)
        user = "root"
        password_file = os.getenv("MARIADB_ROOT_PASSWORD_FILE")
        if password_file:
            with open(password_file, "r") as file:
                password = file.read()
        else:
            password = os.getenv("MARIADB_ROOT_PASSWORD")
        host = os.getenv("MARIADB_HOST")
        database = os.getenv("MARIADB_DATABASE")
        self.client = MariadbClient(
            table,
            user=user,
            password=password,
            host=host,
            database=database,
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        # Create check table
        self._check_table = MariadbTable(
            self.check_table_name, self.check_data_type
        )
        self.logger.info(f"SQL QUERY: {self._check_table.table_query}")
        self.client.execute(self._check_table.table_query)

    def commit(self):
        """Commit to SQL database."""
        self.client.commit()

    def process_item(self, item, spider):
        """
        Process the scraped item and insert it into the MariaDB table.

        Parameters:
            item: The item to be processed.
            spider: The spider instance.

        Returns:
            item: The processed item.
        """
        symbol = item["symbol"]
        data_list = item["data"]
        fields = item["fields"]
        for data in data_list:
            date = data[0]
            formatted_data = {"symbol": symbol}
            formatted_data.update(
                (name, value)
                for name, value in zip(
                    ["year", "month", "day"], date.split("/")
                )
            )
            for name, value in zip(fields[1:], data[1:]):
                if value == "--":
                    formatted_data[name] = None
                else:
                    formatted_data[name] = value.replace(",", "")
            self.client.insert(*formatted_data.values())

        current = datetime.utcnow() + timedelta(hours=8)  # Current taipei time
        year = int(formatted_data["year"])
        month = int(formatted_data["month"])
        if current.year != year or current.month != month:
            self.client.execute(
                self._check_table.insert_query,
                (symbol, year, month, current.strftime("%Y/%m/%d/%H:%M")),
            )
            self.logger.info(f"{symbol} {year}/{month} is done.")
        else:
            self.logger.info(
                f"{symbol} {year}/{month} is current month, "
                "so we need to update everyday."
            )
        self.commit()
        return item

    def close_spider(self, spider):
        """
        Close MariaDB client connection.

        Parameters:
            spider: The spider instance.
        """
        self.client.close()
