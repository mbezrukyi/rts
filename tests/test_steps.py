import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from rts.steps import STEPS


def rows(df: DataFrame) -> list[Row]:
    return sorted(df.collect())


@pytest.mark.parametrize(('format', 'value'), [
    ('json', '{"order_id": "a", "amount": 2}'),
    ('xml', '<order><order_id>a</order_id><amount>2</amount></order>'),
])
def test_parse_spreads_parsed_fields_into_columns(
    spark: SparkSession,
    format: str,
    value: str,
) -> None:
    df = spark.createDataFrame([(1, value.encode())], 'offset int, value binary')

    parsed = STEPS['parse'](format=format, schema='order_id string, amount int')(df)

    assert parsed.columns == ['offset', 'value', 'order_id', 'amount']
    assert parsed.select('offset', 'order_id', 'amount').collect() == [Row(1, 'a', 2)]


def test_filter_keeps_rows_matching_every_condition(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 'x'), (2, 'x'), (3, 'y'), (4, 'z')], 'id int, tag string')

    filtered = STEPS['filter'](conditions=["tag = 'x' or tag = 'y'", 'id > 1'])(df)

    assert rows(filtered) == [Row(2, 'x'), Row(3, 'y')]


def test_join_adds_named_columns_from_the_table(spark: SparkSession) -> None:
    spark.createDataFrame(
        [('s1', 'North', True), ('s2', 'South', False)],
        'store_id string, name string, is_open boolean',
    ).createOrReplaceTempView('stores')
    df = spark.createDataFrame([(1, 's1'), (2, 's2'), (3, 's3')], 'id int, store_id string')

    joined = STEPS['join'](
        table='stores',
        how='left',
        on=['left.store_id = right.store_id', 'right.is_open'],
        columns={'store_name': 'right.name'},
    )(df)

    assert joined.columns == ['id', 'store_id', 'store_name']
    assert rows(joined) == [Row(1, 's1', 'North'), Row(2, 's2', None), Row(3, 's3', None)]


def test_derive_adds_and_replaces_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 2)], 'id int, amount int')

    derived = STEPS['derive'](columns={'amount': 'amount * 10', 'double': 'id * 2'})(df)

    assert derived.collect() == [Row(id=1, amount=20, double=2)]


def test_select_returns_only_named_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, 2, 3)], 'id int, amount int, extra int')

    selected = STEPS['select'](columns={'key': 'id', 'total': 'amount + extra'})(df)

    assert selected.collect() == [Row(key=1, total=5)]


def test_deduplicate_keeps_the_latest_row_per_key(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [('a', 1, 1, 'old'), ('a', 2, 1, 'older tie'), ('a', 2, 2, 'new'), ('b', 1, 1, 'only')],
        'id string, updated int, received int, note string',
    )

    deduplicated = STEPS['deduplicate'](keys=['id'], latest_by=['updated', 'received'])(df)

    assert deduplicated.columns == df.columns
    assert rows(deduplicated) == [Row('a', 2, 2, 'new'), Row('b', 1, 1, 'only')]


def test_aggregate_groups_by_keys(spark: SparkSession) -> None:
    df = spark.createDataFrame([('a', 1), ('a', 5), ('b', 2)], 'id string, amount int')

    aggregated = STEPS['aggregate'](
        keys=['id'],
        columns={'lowest': 'min(amount)', 'total': 'sum(amount)'},
    )(df)

    assert rows(aggregated) == [Row('a', 1, 6), Row('b', 2, 2)]
