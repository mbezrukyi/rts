import posixpath
import tomllib
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any

from .steps import STEPS, Step

type Options = dict[str, str | int | float | bool]


@dataclass(frozen=True)
class KafkaRead:
    options: Options


@dataclass(frozen=True)
class DeltaStreamRead:
    table: str
    options: Options = field(default_factory=dict)


@dataclass(frozen=True)
class DeltaBatchRead:
    table: str


@dataclass(frozen=True)
class DeltaStreamWrite:
    table: str
    output_mode: str
    checkpoint: str
    options: Options = field(default_factory=dict)


@dataclass(frozen=True)
class DeltaMergeWrite:
    table: str
    on: list[str]
    columns: dict[str, str]
    filters: dict[str, str] = field(default_factory=dict)


type Read = KafkaRead | DeltaStreamRead | DeltaBatchRead
type Write = DeltaStreamWrite | DeltaMergeWrite

READS: dict[str, Callable[..., Read]] = {
    'kafka': KafkaRead,
    'delta_stream': DeltaStreamRead,
    'delta_batch': DeltaBatchRead,
}
WRITES: dict[str, Callable[..., Write]] = {
    'delta_stream': DeltaStreamWrite,
    'delta_merge': DeltaMergeWrite,
}


@dataclass(frozen=True)
class Pipeline:
    read: Read
    steps: list[Step]
    write: Write


@dataclass(frozen=True)
class Job:
    name: str
    spark: Options
    pipelines: list[Pipeline]


def load_job(job_path: str, common_path: str) -> Job:
    job = _read_toml(job_path)
    common = _read_toml(common_path)

    return Job(
        name=job['name'],
        spark=common.get('spark', {}),
        pipelines=[
            Pipeline(
                read=_with_common_kafka(
                    _build(READS, pipeline['read']),
                    common.get('kafka', {}),
                ),
                steps=[
                    _build(STEPS, step)
                    for step in pipeline.get('steps', [])
                ],
                write=_under_checkpoint_root(
                    _build(WRITES, pipeline['write']),
                    common.get('checkpoint', ''),
                ),
            )
            for pipeline in job['pipelines']
        ],
    )


def _read_toml(path: str) -> dict[str, Any]:
    with open(path, 'rb') as file:
        return tomllib.load(file)


def _build[Built](
    kinds: dict[str, Callable[..., Built]],
    settings: dict[str, Any],
) -> Built:
    settings = dict(settings)

    return kinds[settings.pop('kind')](**settings)


def _with_common_kafka(read: Read, options: Options) -> Read:
    if isinstance(read, KafkaRead):
        return replace(read, options=options | read.options)

    return read


def _under_checkpoint_root(write: Write, root: str) -> Write:
    if isinstance(write, DeltaStreamWrite):
        return replace(write, checkpoint=posixpath.join(root, write.checkpoint))

    return write
