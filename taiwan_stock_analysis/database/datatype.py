from dataclasses import KW_ONLY, dataclass
from enum import Enum, auto


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
