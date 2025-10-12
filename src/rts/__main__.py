from argparse import ArgumentParser
from functools import reduce

from pyspark.sql import SparkSession

from .config import load_job
from .tables import read, write


def main() -> None:
    parser = ArgumentParser(prog='rts')
    parser.add_argument('--job', required=True)
    parser.add_argument('--common', required=True)
    args = parser.parse_args()

    job = load_job(args.job, args.common)
    spark = (
        SparkSession.builder
        .appName(job.name)
        .config(map=job.spark)
        .getOrCreate()
    )

    for pipeline in job.pipelines:
        write(
            reduce(
                lambda df, step: step(df),
                pipeline.steps,
                read(spark, pipeline.read),
            ),
            pipeline.write,
        )

    while spark.streams.active:
        spark.streams.awaitAnyTermination()
        spark.streams.resetTerminated()


if __name__ == '__main__':
    main()
