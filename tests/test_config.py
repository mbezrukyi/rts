from pathlib import Path

import pytest

from rts.config import (
    DeltaBatchRead,
    DeltaMergeWrite,
    DeltaStreamWrite,
    KafkaRead,
    load_job,
)

EXAMPLES = Path(__file__).parent.parent / 'examples'
COMMON = str(EXAMPLES / 'common.toml')


def test_stream_example_loads_with_common_settings_applied() -> None:
    job = load_job(str(EXAMPLES / 'stream.toml'), COMMON)
    [pipeline] = job.pipelines

    assert job.name == 'order-events'
    assert job.spark == {'spark.sql.shuffle.partitions': 8}
    assert pipeline.read == KafkaRead(
        options={'kafka.bootstrap.servers': 'broker:9092', 'subscribe': 'orders'},
    )
    assert len(pipeline.steps) == 4
    assert pipeline.write == DeltaStreamWrite(
        table='sales.order_events',
        output_mode='append',
        checkpoint='/Volumes/main/sales/checkpoints/order_events',
    )


def test_batch_example_loads() -> None:
    job = load_job(str(EXAMPLES / 'batch.toml'), COMMON)
    [pipeline] = job.pipelines

    assert pipeline.read == DeltaBatchRead(table='sales.order_events')
    assert len(pipeline.steps) == 3
    assert pipeline.write == DeltaMergeWrite(
        table='sales.orders',
        on=['old.order_id = new.order_id'],
        columns={
            'amount': 'new.amount',
            'ordered_at': 'new.ordered_at',
            'ordered_on': 'new.ordered_on',
        },
        filters={'ordered_at': '>'},
    )


def test_unknown_setting_fails_while_loading(tmp_path: Path) -> None:
    job = tmp_path / 'job.toml'
    job.write_text('''
        name = 'job'

        [[pipelines]]
        read = { kind = 'delta_batch', table = 'a.b', tabel = 'typo' }
        write = { kind = 'delta_merge', table = 'a.c', on = [], columns = {} }
    ''')

    with pytest.raises(TypeError, match='tabel'):
        load_job(str(job), COMMON)
