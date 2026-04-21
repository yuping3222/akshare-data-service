import pandas as pd
import pyarrow as pa


class SchemaValidationError(Exception):
    def __init__(self, table: str, errors: list[str]):
        self.table = table
        self.errors = errors
        super().__init__(f"Schema validation failed for '{table}': {'; '.join(errors)}")


PYARROW_TYPE_MAP = {
    "string": pa.string(),
    "int64": pa.int64(),
    "int32": pa.int32(),
    "float64": pa.float64(),
    "float32": pa.float32(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "date64": pa.date64(),
    "timestamp": pa.timestamp("ms"),
    "datetime": pa.timestamp("ms"),
}


class SchemaValidator:
    def __init__(self, table: str, schema: dict[str, str]):
        self.table = table
        self.schema = schema

    def validate(self, df: pd.DataFrame) -> list[str]:
        errors = []
        for col, expected_type in self.schema.items():
            if col not in df.columns:
                errors.append(f"Missing column: '{col}'")
                continue
            actual_dtype = str(df[col].dtype)
            if not SchemaValidator.is_compatible(actual_dtype, expected_type):
                errors.append(
                    f"Column '{col}' has incompatible type: expected '{expected_type}', got '{actual_dtype}'"
                )
        return errors

    def validate_and_cast(
        self, df: pd.DataFrame, primary_key: list[str] | None = None
    ) -> pd.DataFrame:
        errors = self.validate(df)
        if errors:
            raise SchemaValidationError(self.table, errors)

        result = df.copy()
        for col, target_type in self.schema.items():
            if col not in result.columns:
                continue
            actual_dtype = str(result[col].dtype)
            if actual_dtype == target_type:
                continue
            result[col] = self._cast_column(result[col], target_type)

        if primary_key is not None:
            for pk_col in primary_key:
                if pk_col in result.columns and result[pk_col].isna().any():
                    raise SchemaValidationError(
                        self.table,
                        [f"Primary key column '{pk_col}' contains null values"],
                    )

        return result

    def _cast_column(self, series: pd.Series, target_type: str) -> pd.Series:
        if target_type in ("string",):
            return series.astype(str)
        if target_type in ("int64", "int32"):
            return pd.to_numeric(series, errors="coerce").astype(
                "Int64" if target_type == "int64" else "Int32"
            )
        if target_type in ("float64", "float32"):
            return pd.to_numeric(series, errors="coerce").astype(
                "float64" if target_type == "float64" else "float32"
            )
        if target_type in ("date", "date64"):
            return pd.to_datetime(series).dt.date
        if target_type in ("timestamp", "datetime"):
            return pd.to_datetime(series)
        if target_type == "bool":
            return series.astype(bool)
        return series

    @staticmethod
    def is_compatible(from_type: str, to_type: str) -> bool:
        if from_type == to_type:
            return True
        numeric_types = {
            "int64",
            "int32",
            "float64",
            "float32",
            "int8",
            "int16",
            "float16",
        }
        date_types = {"date", "date64", "timestamp", "datetime"}
        from_normalized = from_type.lower()
        to_normalized = to_type.lower()
        if from_normalized in numeric_types and to_normalized in numeric_types:
            return True
        if from_normalized in date_types and to_normalized in date_types:
            return True
        if from_normalized.startswith("datetime64") and to_normalized in date_types:
            return True
        if from_normalized in ("object", "string") and to_normalized in date_types:
            return True
        if from_normalized in ("object", "string") and to_normalized in numeric_types:
            return True
        if from_normalized in numeric_types and to_normalized == "string":
            return True
        if from_normalized in ("object",) and to_normalized == "string":
            return True
        if from_normalized == "bool" and to_normalized in ("string", "int64", "int32"):
            return True
        return False


def infer_schema(df: pd.DataFrame) -> dict[str, str]:
    schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype.startswith("float"):
            schema[col] = "float64"
        elif dtype.startswith("int"):
            schema[col] = "int64"
        elif dtype == "bool":
            schema[col] = "bool"
        elif dtype == "object":
            schema[col] = "string"
        elif "datetime" in dtype:
            schema[col] = "datetime"
        else:
            schema[col] = "string"
    return schema


def normalize_date_columns(
    df: pd.DataFrame, columns: list[str] | None = None
) -> pd.DataFrame:
    result = df.copy()
    cols = columns if columns is not None else result.columns.tolist()
    for col in cols:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col]).dt.date
    return result


def deduplicate_by_key(df: pd.DataFrame, primary_key: list[str]) -> pd.DataFrame:
    return df.drop_duplicates(subset=primary_key, keep="last")
