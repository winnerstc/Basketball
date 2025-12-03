from pyspark.sql.functions import col

def assert_non_empty(df, message="DataFrame is empty"):
    assert df.count() > 0, message

def assert_has_columns(df, required):
    missing = [c for c in required if c not in df.columns]
    assert len(missing) == 0, f"Missing columns: {missing}"

def assert_no_nulls(df, columns):
    for c in columns:
        null_count = df.filter(col(c).isNull()).count()
        assert null_count == 0, f"Null values found in column: {c}"

def assert_value_positive(df, column):
    bad = df.filter(col(column) < 0).count()
    assert bad == 0, f"Negative values in {column}"
