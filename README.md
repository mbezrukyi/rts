# Real-Time Sales Framework

Runs Spark jobs on Databricks that are described in TOML files, not code. A job
is a list of pipelines. Each pipeline reads a Kafka topic or a Delta table,
applies steps in order, and writes a Delta table.

Needs Python 3.12+ and Spark 4+ (Databricks Runtime 17+), which provides
PySpark, Delta and Kafka.

## Run

```sh
rts --job examples/stream.toml --common examples/common.toml
```

On Databricks, build the wheel and run it as a Python wheel task with package
name `rts`, entry point `rts`, and the same parameters.

## Common file

Shared by every job. All keys are optional. See
[examples/common.toml](examples/common.toml).

| Key | Meaning |
|---|---|
| `spark` | Spark session options |
| `kafka` | Kafka options for every `kafka` read; a job's own options win |
| `checkpoint` | Directory that each stream write's `checkpoint` is placed under |

## Job file

`name` is the Spark application name. Each `[[pipelines]]` entry has one
`read`, any number of `steps`, and one `write`, each picked by its `kind`.
See [examples/stream.toml](examples/stream.toml) and
[examples/batch.toml](examples/batch.toml).

### Read

| Kind | Settings |
|---|---|
| `kafka` | `options` |
| `delta_stream` | `table`, optional `options` |
| `delta_batch` | `table` |

### Steps

Every expression and condition is Spark SQL. `columns` maps a column name to
the expression that computes it.

| Kind | Settings | Result |
|---|---|---|
| `parse` | `format` (`json` or `xml`), `schema` (DDL), optional `options` | Fields parsed from `value`, added as columns |
| `filter` | `conditions` | Rows that match every condition |
| `join` | `table`, `how`, `on`, `columns` | All input columns plus `columns`. Conditions and expressions name the input `left` and the table `right` |
| `derive` | `columns` | Columns added or replaced |
| `select` | `columns` | Only `columns` |
| `deduplicate` | `keys`, `latest_by` | Per `keys`, the row with the greatest `latest_by` values |
| `aggregate` | `keys`, `columns` | One row per `keys`, with `columns` as aggregate expressions |

### Write

| Kind | Settings |
|---|---|
| `delta_stream` | `table`, `output_mode`, `checkpoint`, optional `options` |
| `delta_merge` | `table`, `on`, `columns`, optional `filters` |

`delta_merge` updates matched rows only; it inserts nothing. Its conditions and
expressions name the table row `old` and the incoming row `new`. `filters`
maps a column to a comparison operator: a row is updated only if, for some
listed column, `old` is null and `new` is not, or `old <operator> new` holds.

## Test

Needs Java 17 or 21.

```sh
pip install --group test -e .
pytest
```
