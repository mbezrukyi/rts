from pyspark.sql import DataFrame, SparkSession

from .config import (
    DeltaBatchRead,
    DeltaMergeWrite,
    DeltaStreamRead,
    DeltaStreamWrite,
    KafkaRead,
    Read,
    Write,
)

SOURCE = 'source'
OLD = 'old'
NEW = 'new'


def read(spark: SparkSession, read: Read) -> DataFrame:
    match read:
        case KafkaRead():
            return (
                spark.readStream
                .format('kafka')
                .options(**read.options)
                .load()
            )
        case DeltaStreamRead():
            return (
                spark.readStream
                .format('delta')
                .options(**read.options)
                .table(read.table)
            )
        case DeltaBatchRead():
            return spark.read.table(read.table)


def write(df: DataFrame, write: Write) -> None:
    match write:
        case DeltaStreamWrite():
            (
                df.writeStream
                .format('delta')
                .outputMode(write.output_mode)
                .options(**write.options)
                .toTable(write.table, checkpointLocation=write.checkpoint)
            )
        case DeltaMergeWrite():
            df.createOrReplaceTempView(SOURCE)
            df.sparkSession.sql(merge_sql(write))


def merge_sql(merge: DeltaMergeWrite) -> str:
    on = ' and '.join(merge.on)
    updates = ', '.join(
        f'{OLD}.{column} = {expression}'
        for column, expression in merge.columns.items()
    )
    changed = ' or '.join(
        f'({OLD}.{column} is null and {NEW}.{column} is not null)'
        f' or {OLD}.{column} {operator} {NEW}.{column}'
        for column, operator in merge.filters.items()
    )
    matched = f'when matched and ({changed})' if changed else 'when matched'

    return (
        f'merge into {merge.table} as {OLD} using {SOURCE} as {NEW} '
        f'on {on} {matched} then update set {updates}'
    )
