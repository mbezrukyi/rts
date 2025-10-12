from collections.abc import Callable

from pyspark.sql import Column, DataFrame, Window, functions as F

type Step = Callable[[DataFrame], DataFrame]
type Expressions = dict[str, str]

LEFT = 'left'
RIGHT = 'right'
PARSED = 'parsed'
RANK = 'rank'
PARSERS = {'json': F.from_json, 'xml': F.from_xml}


def _named(columns: Expressions) -> list[Column]:
    return [
        F.expr(expression).alias(name)
        for name, expression in columns.items()
    ]


def _parse(format: str, schema: str, options: dict[str, str] = {}) -> Step:
    parse = PARSERS[format]

    return lambda df: (
        df
        .withColumn(PARSED, parse(F.col('value').cast('string'), schema, options))
        .select('*', f'{PARSED}.*')
        .drop(PARSED)
    )


def _filter(conditions: list[str]) -> Step:
    return lambda df: df.filter(
        ' and '.join(f'({condition})' for condition in conditions)
    )


def _join(table: str, how: str, on: list[str], columns: Expressions) -> Step:
    return lambda df: (
        df.alias(LEFT)
        .join(
            df.sparkSession.table(table).alias(RIGHT),
            on=[F.expr(condition) for condition in on],
            how=how,
        )
        .select(f'{LEFT}.*', *_named(columns))
    )


def _derive(columns: Expressions) -> Step:
    return lambda df: df.withColumns(dict(zip(columns, _named(columns))))


def _select(columns: Expressions) -> Step:
    return lambda df: df.select(*_named(columns))


def _deduplicate(keys: list[str], latest_by: list[str]) -> Step:
    return lambda df: (
        df
        .withColumn(RANK, F.row_number().over(
            Window
            .partitionBy(*keys)
            .orderBy(*map(F.desc, latest_by))
        ))
        .filter(F.col(RANK) == 1)
        .drop(RANK)
    )


def _aggregate(keys: list[str], columns: Expressions) -> Step:
    return lambda df: df.groupBy(*keys).agg(*_named(columns))


STEPS: dict[str, Callable[..., Step]] = {
    'parse': _parse,
    'filter': _filter,
    'join': _join,
    'derive': _derive,
    'select': _select,
    'deduplicate': _deduplicate,
    'aggregate': _aggregate,
}
